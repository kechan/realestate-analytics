#!/bin/bash

# Define the full path to the Python interpreter in your virtual environment
PYTHON_PATH="/home/jupyter/LocalLogicRewrite/llr_venv/bin/python"

# Define the base path for the Python scripts and configuration files
SCRIPT_PATH="/home/jupyter/Analytics/realestate-analytics/scripts"
CONFIG_PATH="/home/jupyter/Analytics/realestate-analytics/scripts"

# Run the Python scripts with the specified interpreter and configuration files
$PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
$PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py --config $CONFIG_PATH/uat_config.yaml
$PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py --config $CONFIG_PATH/uat_config.yaml --sim_success_at_post_load
$PYTHON_PATH $SCRIPT_PATH/run_nearby_comparable_solds.py --config $CONFIG_PATH/uat_config.yaml

$PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
$PYTHON_PATH $SCRIPT_PATH/run_historic_metrics.py --config $CONFIG_PATH/uat_config.yaml

$PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py --config $CONFIG_PATH/prod_config.yaml --sim_failure_at_pre_transform
$PYTHON_PATH $SCRIPT_PATH/run_last_mth_metrics.py --config $CONFIG_PATH/uat_config.yaml

$PYTHON_PATH $SCRIPT_PATH/run_current_mth_metrics.py --config $CONFIG_PATH/uat_config.yaml

$PYTHON_PATH $SCRIPT_PATH/run_absorption_rate.py --config $CONFIG_PATH/uat_config.yaml

