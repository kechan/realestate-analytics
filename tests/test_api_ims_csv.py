"""Read-only CSV preview, direct mapper reuse, and provincial-rule drift checks."""
import ast
import io
import tempfile
from types import SimpleNamespace
from pathlib import Path
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from realestate_analytics.api.ims_csv_preview import csv_batch_preview, csv_preview, InvalidCSV, MappingFilesUnavailable
from realestate_analytics.api.config import Settings
from realestate_analytics.api.ims_mapping_schemas import MappingReport
from realestate_analytics.api.ims_mapper_adapter import ReadOnlyIMSMapper, apply_provincial_exception
from realestate_analytics.utils.ims import IMSPropertyTypeMapper


class MapperReuseTests(TestCase):
    def test_adapter_inherits_computation_without_reimplementing_it(self):
        self.assertIs(ReadOnlyIMSMapper.suggest_mapping, IMSPropertyTypeMapper.suggest_mapping)
        self.assertIs(ReadOnlyIMSMapper.generate_full_mapping_dataframe, IMSPropertyTypeMapper.generate_full_mapping_dataframe)
        self.assertIs(ReadOnlyIMSMapper.apply_property_types, IMSPropertyTypeMapper.apply_property_types)
        self.assertIs(ReadOnlyIMSMapper._suggest_mapping_refined, IMSPropertyTypeMapper._suggest_mapping_refined)
        mapper = ReadOnlyIMSMapper()
        with self.assertRaises(PermissionError):
            mapper.save_mapping()

    def test_only_exception_matches_both_live_production_provincial_blocks(self):
        source = Path(__file__).resolve().parents[1] / "realestate_analytics/data/es.py"
        tree = ast.parse(source.read_text())
        datastore = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Datastore")
        for name in ("bulk_import_ims_sold_listings", "bulk_update_ims_sold_listings"):
            method = next(node for node in datastore.body if isinstance(node, ast.FunctionDef) and node.name == name)
            mask = next(node for node in ast.walk(method) if isinstance(node, ast.Assign) and
                        any(isinstance(target, ast.Name) and target.id == "ab_duplex_mask" for target in node.targets))
            use = next(node for node in ast.walk(method) if isinstance(node, ast.If) and isinstance(node.test, ast.Call)
                       and isinstance(node.test.func, ast.Attribute) and isinstance(node.test.func.value, ast.Name)
                       and node.test.func.value.id == "ab_duplex_mask")
            frame = pd.DataFrame({"Province": np.repeat(["AB", "ON", "ab", None], 6),
                                  "SubType": ["Duplex", "Full Duplex", "Half Duplex", "Duplex Side By Side", "Detached", None] * 4,
                                  "propertyType": "OTHER"})
            expected = frame.copy()
            namespace = {"df": expected, "self": SimpleNamespace(logger=MagicMock())}
            # Only the mask and its dataframe update execute; no datastore is
            # imported or instantiated, and no import/ES routine is called.
            exec(compile(ast.fix_missing_locations(ast.Module(body=[mask, use], type_ignores=[])), "production-provincial-rule", "exec"), namespace)
            apply_provincial_exception(frame)
            pd.testing.assert_frame_equal(frame, expected, obj=f"Provincial exception drifted from {name}; see TODO.md and AGENTS.md")


class CSVPreviewTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.home() / "tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.base = self.root / "ims_property_type_mapping_v4_df"
        self.overrides = self.root / "ims_property_type_lookup_df"
        pd.DataFrame([
            ["Residential", "Detached", "2 Storey", "OTHER"],  # correction in saved base must win over fresh algorithm
            ["Residential", "Duplex", "Bungalow", "OTHER"],
        ], columns=["Type", "SubType", "Style", "Inferred"]).to_feather(self.base)
        pd.DataFrame([
            ["Residential", "Duplex", "Bungalow", "DETACHED"],
            ["Residential", "Recreational", "Park Model", "OTHER"],
            [None, None, None, None],
        ], columns=["Type", "SubType", "Style", "propertyType"]).to_feather(self.overrides)

    def run_csv(self, text):
        return csv_preview(io.BytesIO(text.encode("utf-8")), self.root)

    def test_cache_first_new_algorithm_manual_and_provincial_rules_preserve_counts(self):
        original = {p.name: p.read_bytes() for p in self.root.iterdir()}
        result = self.run_csv('Type,SubType,Style,Province,propertyType,Address\n'
            'Residential, Detached , 2 Storey ,ON,CONDO,ignored\n'
            'Residential,Detached,2 Storey,ON,CONDO,ignored\n'
            'Residential,Duplex,Bungalow,ON,CONDO,ignored\n'
            'Residential,Duplex,Bungalow,AB,CONDO,ignored\n'
            'Residential,Recreational,Park Model,ON,CONDO,ignored\n'
            'Appartement,N/R,Apartment,QC,OTHER,ignored\n')
        MappingReport.model_validate(result)
        self.assertEqual(result["document_count"], 6)
        self.assertEqual(result["new_tuple_count"], 2)
        self.assertEqual(result["conflict_tuple_count"], 1)
        breakdown = result["province_breakdown"]
        self.assertEqual(breakdown["conflict_tuple_count"], 0)
        self.assertEqual(breakdown["tuple_count"], 5)
        self.assertEqual(sum(row["count"] for row in breakdown["rows"]), result["document_count"])
        self.assertEqual(result["report_mode"], "csv")
        self.assertEqual(result["scan_slices"], 0)
        rows = pd.DataFrame(result["rows"])
        cached = rows.loc[rows["SubType"].eq("Detached")].iloc[0]
        self.assertEqual((cached.property_type, cached["count"], cached.base_origin), ("OTHER", 2, "cache"))
        duplex = rows.loc[rows["SubType"].eq("Duplex")].set_index("property_type")
        self.assertEqual(duplex.loc["SEMI-DETACHED", "mapping_source"], "provincial_rule")
        self.assertEqual(duplex.loc["DETACHED", "mapping_source"], "manual_override")
        provincial_duplex = {
            row["Province"]: (row["property_type"], row["mapping_source"])
            for row in breakdown["rows"] if row["SubType"] == "Duplex"
        }
        self.assertEqual(provincial_duplex, {
            "AB": ("SEMI-DETACHED", "provincial_rule"),
            "ON": ("DETACHED", "manual_override"),
        })
        park = rows.loc[rows["Style"].eq("Park Model")].iloc[0]
        self.assertEqual((park.algorithm_mapping, park.override_mapping, park.property_type), ("MOBILE_MINI", "OTHER", "OTHER"))
        apartment = rows.loc[rows["Type"].eq("Appartement")].iloc[0]
        self.assertEqual((apartment.property_type, apartment.mapping_source, apartment.lineage_status), ("CONDO", "algorithm_rules", "computed"))
        self.assertEqual({p.name: p.read_bytes() for p in self.root.iterdir()}, original)

    def test_province_breakdown_preserves_rows_with_unknown_province(self):
        header = "Type,SubType,Style,Province\n"
        rows = "Residential,Detached,2 Storey,\nResidential,Detached,2 Storey,  \n"
        self.assertNotIn("province_breakdown", self.run_csv(header + rows))
        result = self.run_csv(header + rows + "Residential,Detached,2 Storey,QC\n")
        breakdown = result["province_breakdown"]
        self.assertEqual({row["Province"]: row["count"] for row in breakdown["rows"]}, {None: 2, "QC": 1})
        self.assertEqual(result["rows"][0]["count"], 3)

    def test_csv_headers_bom_and_quoted_commas(self):
        result = self.run_csv('\ufeff Type , SubType , Style , Province \nResidential,Detached,"Attached, Multi Level",ON\n')
        self.assertEqual(result["rows"][0]["Style"], "Attached, Multi Level")
        self.assertEqual(result["rows"][0]["property_type"], "TOWNHOUSE")

    def test_batch_keeps_last_listing_before_mapping_and_grouping(self):
        header = "Type,SubType,Style,Province,MLSID,MLSNumber,WrittenDate\n"
        original = header + (
            "Obsolete,Detached,Bungalow,on,01,a007,2023-01-01T00:00:00\n"
            "Residential,Detached,2 Storey,ON,02,A007,2023-01-01\n"
            "Residential,Detached,2 Storey,ON,01,A007,2022-01-01\n"
            "Residential,Detached,2 Storey,BC,01,A007,2023-01-01\n"
            "Residential,Detached,2 Storey,ON,01,,2023-01-01"
        )
        amended = "\ufeff" + header + (
            "Obsolete,Detached,Bungalow,ON,01,A007,2023-02-01\n"
            "Residential,Duplex,Bungalow,ON,01,A007,2023-02-02\n"
            "Residential,Detached,2 Storey,ON,01,,2023-01-01\n"
        )
        result = csv_batch_preview([io.BytesIO(item.encode("utf-8")) for item in (original, amended)], self.root)
        self.assertEqual((result["csv_input_row_count"], result["document_count"], result["csv_unidentified_row_count"]), (8, 6, 2))
        self.assertEqual(result["new_tuple_count"], 0)
        self.assertEqual({(row["SubType"], row["property_type"]): row["count"] for row in result["rows"]},
                         {("Detached", "OTHER"): 5, ("Duplex", "DETACHED"): 1})

    def test_batch_requires_listing_columns_in_every_file(self):
        data = b"Type,SubType,Style,Province\nResidential,Detached,Bungalow,ON\n"
        with self.assertRaisesRegex(InvalidCSV, "CSV 1: Missing required columns: MLSID, MLSNumber, WrittenDate"):
            csv_batch_preview([io.BytesIO(data), io.BytesIO(data)], self.root)

    def test_current_mapper_and_override_implementation_are_called_directly(self):
        with patch.object(IMSPropertyTypeMapper, "suggest_mapping", new=lambda self, *args: "LAND_VACANT"):
            result = self.run_csv('Type,SubType,Style,Province\nNew source,New subtype,New style,ON\n')
            self.assertEqual(result["rows"][0]["property_type"], "LAND_VACANT")
        original = IMSPropertyTypeMapper.apply_property_types
        def revised_apply(mapper, df=None):
            original(mapper, df)
            mapper.df["propertyType"] = "CONDO"
        with patch.object(IMSPropertyTypeMapper, "apply_property_types", new=revised_apply):
            result = self.run_csv('Type,SubType,Style,Province\nResidential,Duplex,Bungalow,ON\n')
            # The API must not reapply its own manual-override interpretation.
            self.assertEqual(result["rows"][0]["property_type"], "CONDO")

    def test_missing_null_cache_values_are_not_reinferred(self):
        pd.DataFrame([["Residential", "Detached", "2 Storey", None]],
                     columns=["Type", "SubType", "Style", "Inferred"]).to_feather(self.base)
        result = self.run_csv('Type,SubType,Style,Province\nResidential,Detached,2 Storey,ON\n,,,ON\n')
        self.assertEqual(result["document_count"], 2)
        self.assertEqual(result["new_tuple_count"], 1)
        self.assertEqual(result["missing_property_type_count"], 1)
        self.assertIsNone(next(row for row in result["rows"] if row["Type"] == "Residential")["property_type"])

    def test_bad_empty_or_incomplete_csv_is_rejected(self):
        for text in ('', 'Type,SubType,Style,Province\n', 'Type,SubType,Style\nA,B,C\n',
                     'Type, Type ,SubType,Style,Province\nA,B,C,D,E\n', 'Type,SubType,Style,Province\n"unterminated'):
            with self.subTest(text=text), self.assertRaises(InvalidCSV):
                self.run_csv(text)

    def test_missing_mapping_files_fail_instead_of_silently_changing_the_pipeline(self):
        self.overrides.unlink()
        with self.assertRaises(MappingFilesUnavailable):
            self.run_csv('Type,SubType,Style,Province\nResidential,Detached,Bungalow,ON\n')


class CSVRouteTests(IsolatedAsyncioTestCase):
    async def test_successful_upload_uses_local_files_and_never_calls_es(self):
        import httpx
        from realestate_analytics.api.main import app
        with tempfile.TemporaryDirectory(dir=Path.home() / "tmp") as folder:
            root = Path(folder)
            pd.DataFrame([["Residential", "Detached", "Bungalow", "DETACHED"]],
                         columns=["Type", "SubType", "Style", "Inferred"]).to_feather(root / "ims_property_type_mapping_v4_df")
            pd.DataFrame(columns=["Type", "SubType", "Style", "propertyType"]).reset_index(drop=True).to_feather(root / "ims_property_type_lookup_df")
            es = MagicMock()
            app.state.repository = SimpleNamespace(client=es, settings=Settings(api_key="unused", analytics_cache_dir=root))
            uploads = []

            def capture_spool(*, max_size, **kwargs):
                stream = tempfile.SpooledTemporaryFile(max_size=min(16, max_size), **kwargs)
                uploads.append(stream)
                return stream

            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
                with patch("realestate_analytics.api.main.SpooledTemporaryFile", side_effect=capture_spool):
                    response = await http.post("/api/v1/ims-mappings/preview", content=b"Type,SubType,Style,Province\nResidential,Detached,Bungalow,ON\n", headers={"Content-Type": "text/csv"})
                    header = b"Type,SubType,Style,Province,MLSID,MLSNumber,WrittenDate\n"
                    first = header + b"Obsolete,Detached,Bungalow,ON,1,A,2023-01-01"
                    last = b"\xef\xbb\xbf" + header + b"Residential,Detached,Bungalow,ON,1,A,2023-01-02\n"

                    async def body():
                        yield first[:10]
                        yield first[10:] + last[:7]
                        yield last[7:]

                    headers = {"Content-Type": "application/vnd.ims.csv-batch",
                               "X-IMS-CSV-Sizes": f"{len(first)},{len(last)}"}
                    batch = await http.post("/api/v1/ims-mappings/preview", content=body(), headers=headers)
                    for malformed in (first + last[:-1], first + last + b"extra"):
                        mismatch = await http.post("/api/v1/ims-mappings/preview", content=malformed, headers=headers)
                        self.assertEqual(mismatch.status_code, 422, mismatch.text)
            self.assertEqual(response.status_code, 200, response.text)
            result = MappingReport.model_validate(response.json())
            self.assertEqual(result.rows[0].property_type, "DETACHED")
            self.assertEqual(result.report_mode, "csv")
            self.assertEqual(result.document_count, 1)
            self.assertEqual(batch.status_code, 200, batch.text)
            combined = MappingReport.model_validate(batch.json())
            self.assertEqual((combined.csv_input_row_count, combined.document_count), (2, 1))
            self.assertEqual(combined.rows[0].property_type, "DETACHED")
            self.assertEqual(len(uploads), 7)
            self.assertTrue(all(stream.name is not None and stream.closed for stream in uploads))
            self.assertEqual(es.mock_calls, [])

    async def test_raw_upload_has_no_es_calls_and_cleans_up_on_bad_input(self):
        import httpx
        from realestate_analytics.api.main import app
        es = MagicMock()
        app.state.repository = SimpleNamespace(client=es, settings=Settings(api_key="unused"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            result = await http.post("/api/v1/ims-mappings/preview", content=b"bad header\n", headers={"Content-Type": "text/csv"})
            self.assertEqual(result.status_code, 422, result.text)
            self.assertIn("Missing required columns", result.json()["detail"])
            self.assertEqual((await http.post("/api/v1/ims-mappings/preview", json={})).status_code, 415)
            headers = {"Content-Type": "application/vnd.ims.csv-batch", "X-IMS-CSV-Sizes": "1073741824,10"}
            with patch("realestate_analytics.api.main.disk_usage", return_value=SimpleNamespace(free=2**32)):
                result = await http.post("/api/v1/ims-mappings/preview", content=b"short", headers=headers)
                self.assertEqual(result.status_code, 422, result.text)
                self.assertIn("byte lengths do not match", result.json()["detail"])
            with patch("realestate_analytics.api.main.disk_usage", return_value=SimpleNamespace(free=2**20)):
                result = await http.post("/api/v1/ims-mappings/preview", content=b"short", headers=headers)
                self.assertEqual(result.status_code, 507, result.text)
                self.assertIn("temporary disk space", result.json()["detail"])
        self.assertEqual(es.mock_calls, [])
