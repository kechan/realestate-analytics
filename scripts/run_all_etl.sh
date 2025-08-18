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
  echo "=== $PROV : prod config with simulated failure ==="
  $PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py \
    --config $CONFIG_PATH/prod_config.yaml \
    --prov_code $PROV \
    --sim_failure_at_pre_transform

  echo "=== $PROV : UAT config ==="
  $PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py \
    --config $CONFIG_PATH/uat_config.yaml \
    --prov_code $PROV

  echo "=== $PROV : UAT config with simulated success ==="
  $PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py \
    --config $CONFIG_PATH/uat_config.yaml \
    --prov_code $PROV \
    --sim_success_at_post_load

  echo "=== $PROV : UAT config (default) ==="
  $PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py \
    --config $CONFIG_PATH/uat_config.yaml \
    --prov_code $PROV
done

# $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
# $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py --config $CONFIG_PATH/uat_config.yaml

for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "=== $PROV : historic metrics - prod config with simulated failure ==="
  $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py \
    --config $CONFIG_PATH/prod_config.yaml \
    --prov_code $PROV \
    --sim_failure_at_pre_transform
  
  echo "=== $PROV : historic metrics - UAT config ==="
  $PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py \
    --config $CONFIG_PATH/uat_config.yaml \
    --prov_code $PROV
done

# $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
# $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py --config $CONFIG_PATH/uat_config.yaml

# ───────────────────────────────────────────────────────────────
#  Last-month metrics: prod-sim-failure + UAT, per province
# ───────────────────────────────────────────────────────────────
for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "=== $PROV : prod config with simulated failure ==="
  $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py \
      --config $CONFIG_PATH/prod_config.yaml \
      --prov_code $PROV \
      --sim_failure_at_pre_transform

  echo "=== $PROV : UAT config ==="
  $PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py \
      --config $CONFIG_PATH/uat_config.yaml \
      --prov_code $PROV
done


$PYTHON_PATH $SCRIPT_PATH/run_current_mth_metrics.py --config $CONFIG_PATH/uat_config.yaml

# $PYTHON_PATH $SCRIPT_PATH/run_absorption_rate.py --config $CONFIG_PATH/uat_config.yaml
for PROV in ON BC AB SK MB QC NB NS PE NL YT NT NU; do
  echo "=== $PROV : prod config with simulated failure ==="
  $PYTHON_PATH $SCRIPT_PATH/run_absorption_rate.py \
    --config $CONFIG_PATH/prod_config.yaml \
    --prov_code $PROV \
    --sim_failure_at_pre_transform
  echo "=== $PROV : UAT config ==="
  $PYTHON_PATH $SCRIPT_PATH/run_absorption_rate.py \
    --config $CONFIG_PATH/uat_config.yaml \
    --prov_code $PROV
done

