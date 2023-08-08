#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate

cd /home/xeniaprod/scripts/Florida-Water-Quality/scripts;

python florida_wq_harvest_bacteria_data.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_wq_harvest_bacteria_data.ini --SiteAliases=/home/xeniaprod/scripts/Florida-Water-Quality/config/all_data_sites_mapping.json > /home/xeniaprod/tmp/log/florida_harvest_bacteria_data_sh.log 2>&1