#!/bin/bash

# Define the full path to the Python interpreter in your virtual environment
PYTHON_PATH="/home/jupyter/LocalLogicRewrite/llr_venv/bin/python"

# Define the base path for the Python scripts and configuration files
SCRIPT_PATH="/home/jupyter/Analytics/realestate-analytics/scripts"
CONFIG_PATH="/home/jupyter/Analytics/realestate-analytics/scripts"

# Run the Python scripts with the specified interpreter and configuration files

# ───────────────────────────────────────────────────────────────
# Nearby Comparable Solds: per-province runs
# ───────────────────────────────────────────────────────────────
for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "=== Nearby Comparable Solds : $PROV ==="
  $PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py \
    --config $CONFIG_PATH/prod_config.yaml \
    --prov_code $PROV
done

# $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
# $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py --config $CONFIG_PATH/uat_config.yaml

for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "=== Historic Metrics: $PROV ==="
  $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py \
    --config $CONFIG_PATH/prod_config.yaml \
    --prov_code $PROV
done

# $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
# $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py --config $CONFIG_PATH/uat_config.yaml

# ───────────────────────────────────────────────────────────────
#  Last-month metrics: per province
# ───────────────────────────────────────────────────────────────
for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "--- Last Month Metrics: $PROV ---"
  $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py \
      --config $CONFIG_PATH/prod_config.yaml \
      --prov_code $PROV
done


# ═══════════════════════════════════════════════════════════════
# CURRENT MONTH METRICS: All provinces (current listings only)
# ═══════════════════════════════════════════════════════════════
$PYTHON_PATH $SCRIPT_PATH/run_current_mth_metrics.py --config $CONFIG_PATH/prod_config.yaml


# ═══════════════════════════════════════════════════════════════
# ABSORPTION RATE: All provinces (current + sold data)
# ═══════════════════════════════════════════════════════════════
for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "=== Absorption Rate: $PROV ==="
  $PYTHON_PATH $SCRIPT_PATH/run_absorption_rate.py \
    --config $CONFIG_PATH/prod_config.yaml \
    --prov_code $PROV
done

