source /usr2/virtualenvs/pyenv2.7/bin/activate

python florida_wq_processing.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_config.ini --BuildSummaryData --WaterQualityHistoricalFile="/home/xeniaprod/scripts/Florida-Water-Quality/data/sampling_data/SarasotaManatee Data.csv" --HistoricalSummaryHeaderRow=autonumber,PeriodID,County,SPLocation,SPNo,Date,SampleTime,enterococcus,fecalColiform,PoorResults,advisory,WaterQuality,CollectionTime,Collector,AirTemp,WaterTemp,Weather,RainFall24h,RainFall3d,RainLastWeek,TidalConditionsA,TidalConditionsB,CurrentDirectionA,CurrentDirectionB,CurrentStrength,CommentField,Resample,Updated,enterococcus_code,fecalColiform_code,ec_GeoMean,ec_GeoMean_Code,SPID,Value_Qual,Batch_ID,Analysis_date,Analysis_Time,Lab_ID,Value_Qual_FC,Batch_ID_FC,Analysis_date_FC,Analysis_Time_FC,MDL_EC,MDL_FC,uql,uql_fc,, --HistoricalSummaryOutPath=/home/xeniaprod/scripts/Florida-Water-Quality/data/sampling_data/



nohup python wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_config.ini --ImportData=/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201301/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201302/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201303/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201304/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201305/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201306/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201307/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201308/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201309/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201310/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201311/,/mnt/xmrgdata/xmrgdata/historical/2013/qpexmrg_201312