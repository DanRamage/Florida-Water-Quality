#!/bin/bash

source /usr2/virtualenvs/pyenv2.7/bin/activate

python /home/xeniaprod/scripts/Florida-Water-Quality/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_config.ini --FillGaps --BackfillNHours=192