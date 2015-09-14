#!/bin/bash

source /usr2/virtualenvs/pyenv2.7/bin/activate;

cd /home/xeniaprod/scripts/Florida-Water-Quality/scripts;

python florida_wq_prediction_engine.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_prediction_config.ini > /home/xeniaprod/tmp/log/florida_wq_prediction_engine_sh.log 2>&1
