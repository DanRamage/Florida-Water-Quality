library(optparse)
library(raster)



print("start")

#option_list <- list(
#  make_option("--inifile", default="", metavar="ini file",
#              help="INI file for station.")
#)
# get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults,
#opt <- parse_args(OptionParser(option_list=option_list))

#cat("INI File: ",  opt$inifile)
#print("\n")

ini_settings<-readIniFile("/Users/danramage/Documents/workspace/WaterQuality/Florida_Water_Quality/data/model_files/R/lido_model_validate.ini", token='=', commenttoken=';', aslist=TRUE)

model_count <- strtoi(ini_settings$settings$model_count)

for (model_num in 1:model_count)
{
  model_section <- paste("model_", model_num, sep="")
  print(paste("Model Section: ", model_section, sep=""))

  data_file <- ini_settings[[model_section]]$input_data
  print(paste("Data File: ", data_file, sep=""))
  
}

print(ini_settings)

print("finish")

