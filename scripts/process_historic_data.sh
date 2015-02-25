source /usr2/virtualenvs/pyenv2.7/bin/activate

python florida_wq_processing.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_config.ini --BuildSummaryData --WaterQualityHistoricalFile="/home/xeniaprod/scripts/Florida-Water-Quality/data/sampling_data/SarasotaManatee Data.csv" --HistoricalSummaryHeaderRow=autonumber,PeriodID,County,SPLocation,SPNo,Date,SampleTime,enterococcus,fecalColiform,PoorResults,advisory,WaterQuality,CollectionTime,Collector,AirTemp,WaterTemp,Weather,RainFall24h,RainFall3d,RainLastWeek,TidalConditionsA,TidalConditionsB,CurrentDirectionA,CurrentDirectionB,CurrentStrength,CommentField,Resample,Updated,enterococcus_code,fecalColiform_code,ec_GeoMean,ec_GeoMean_Code,SPID,Value_Qual,Batch_ID,Analysis_date,Analysis_Time,Lab_ID,Value_Qual_FC,Batch_ID_FC,Analysis_date_FC,Analysis_Time_FC,MDL_EC,MDL_FC,uql,uql_fc,, --HistoricalSummaryOutPath=/home/xeniaprod/scripts/Florida-Water-Quality/data/sampling_data/


nohup python wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/Florida-Water-Quality/config/florida_config.ini --ImportData=/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201101/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201102/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201103/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201104/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201105/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201106/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201107/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201108/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201109/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201110/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201111/,/mnt/xmrgdata/xmrgdata/historical/2011/qpexmrg_2011/qpexmrg_201112/