MKT_TRENDS_INDEX_MAPPING = {
  "mappings": {
    "properties": {
      "geog_id": {"type": "keyword"},
      "propertyType": {"type": "keyword"},
      "geo_level": {"type": "integer"},
      "metrics": {
        "properties": {
          "median_sold_price": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "median_dom": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "absorption_rate": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "months_of_inventory": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "last_mth_median_asking_price": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "last_mth_new_listings": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "long"}
            }
          },
          "over_ask_percentage": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "below_ask_percentage": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "float"}
            }
          },
          "sold_listing_count": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "integer"}
            }
          },
          "current_metrics": {
            "properties": {
              "median_asking_price": {"type": "float"},
              "new_listings": {"type": "integer"}
            }
          },
          "mth_end_snapshot_listing_count": {
            "type": "nested",
            "properties": {
              "month": {"type": "date", "format": "yyyy-MM"},
              "value": {"type": "integer"}
            }
          }
        }
      },
      "last_updated": {"type": "date"}
    }
  }
}


IMS_SOLD_LISTING_INDEX_MAPPING = {
  "mappings": {
    "properties": {
      # Core Transaction Fields
      "WrittenDate": {"type": "date", "format": "yyyy-MM-dd"},
      "ListingDate": {"type": "date", "format": "yyyy-MM-dd"},
      "Province": {"type": "keyword"},
      "SoldPrice": {"type": "float"},
      "ListPrice": {"type": "float"},
      "DaysOnMarket": {"type": "integer"},

      # Geographic Fields
      "Latitude": {"type": "float"},
      "Longitude": {"type": "float"},
      "City": {"type": "keyword"},
      "FSACity": {"type": "keyword"},
      "County": {"type": "keyword"},
      "CityProvince": {"type": "keyword"},
      "Community": {"type": "text"},
      "District": {"type": "keyword"},
      "Country": {"type": "keyword"},
      "PostalCode": {"type": "keyword"},
      "FSA": {"type": "keyword"},
      "guid": {"type": "text"},

      # Property Characteristics
      "Bedrooms": {"type": "integer"},
      "Baths": {"type": "float"},
      "Type": {"type": "keyword"},
      "SubType": {"type": "text"},
      "Style": {"type": "text"},
      "SquareFoot": {"type": "integer"},
      "YearBuilt": {"type": "keyword"},
      "Garage": {"type": "integer"},
      "CLASA": {"type": "keyword"},
      "propertyType": {"type": "keyword"},

      # Computed/Bonus Fields
      "SoldOverAsk": {"type": "boolean"},
      "ListingSellingPriceDiff": {"type": "float"},
      "ListPricePercentOfSold": {"type": "keyword"},
      "SoldPricePercentOfList": {"type": "keyword"},
      "IsLuxuryListed": {"type": "boolean"},
      "IsLuxurySold": {"type": "boolean"},

      # Identifiers
      "MLSNumber": {"type": "keyword"},
      "MLSID": {"type": "keyword"},
      "Address": {"type": "text"},

      # System Fields
      "last_updated": {"type": "date"}
    }
  }
}
