import sys
sys.path.append('../commonfiles/python')

import logging.config
from datetime import datetime, timedelta
from pytz import timezone
import optparse
import ConfigParser
import csv

from collections import OrderedDict
from wqHistoricalData import station_geometry,sampling_sites, wq_defines, geometry_list, tide_data_file
from florida_wq_data import florida_wq_historical_data, florida_sample_sites

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import path




def create_historical_summary(config_file_name,
                              historical_wq_file,
                              header_row,
                              summary_out_file,
                              starting_date,
                              start_time_midnight,
                              tide_csv_file,
                              use_logger=False):
  logger = None
  if use_logger:
    logger = logging.getLogger('create_historical_summary_logger')
  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(config_file_name)

    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    xenia_db_file = config_file.get('database', 'name')
    c_10_tds_url = config_file.get('c10_data', 'historical_qaqc_thredds')
    hycom_model_tds_url = config_file.get('hycom_model_data', 'thredds_url')
    model_bbox = config_file.get('hycom_model_data', 'bbox').split(',')
    poly_parts = config_file.get('hycom_model_data', 'within_polygon').split(';')
    model_within_polygon = [(float(lon_lat.split(',')[0]), float(lon_lat.split(',')[1])) for lon_lat in poly_parts]

    ncsu_model_tds_url = config_file.get('ncsu_model_data', 'thredds_url')

  except ConfigParser, e:
    if logger:
      logger.exception(e)
  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    #If we provide a tide file that has the historical data, we'll load it instead of using
    #the SOAP webservice.
    tide_file = None
    if tide_csv_file is not None and len(tide_csv_file):
      tide_file = tide_data_file(logger=True)
      tide_file.open(tide_csv_file)
    try:
      wq_file = open(historical_wq_file, "rU")
      wq_history_file = csv.DictReader(wq_file, delimiter=',', quotechar='"', fieldnames=header_row)
    except IOError, e:
      if logger:
        logger.exception(e)
    else:
      line_num = 0
      #Dates in the spreadsheet are stored in EST. WE want to work internally in UTC.
      eastern = timezone('US/Eastern')

      output_keys = ['station_name', 'sample_date', 'enterococcus_value', 'enterococcus_code', 'autonumber', 'County']

      sites_not_found = []
      current_site = None
      processed_sites = []
      #stop_date = eastern.localize(datetime.strptime('2014-01-29 00:00:00', '%Y-%m-%d %H:%M:%S'))
      #stop_date = stop_date.astimezone(timezone('UTC'))
      try:
        fl_wq_data = florida_wq_historical_data(xenia_database_name=xenia_db_file,
                                      c_10_tds_url=c_10_tds_url,
                                      hycom_model_tds_url=hycom_model_tds_url,
                                      ncsu_model_tds_url=ncsu_model_tds_url,
                                      model_bbox=model_bbox,
                                      model_within_polygon=model_within_polygon,
                                      use_logger=True)
      except Exception, e:
        if logger:
          logger.exception(e)
      else:
        if logger:
          logger.info("Begin looping through file: %s" % (historical_wq_file))
        for row in wq_history_file:
          #Check to see if the site is one we are using
          if line_num > 0:
            cleaned_site_name = row['SPLocation'].replace("  ", " ")

            date_val = row['Date']
            time_val = row['SampleTime']
            if len(date_val):
              #Date does not have leading 0s sometimes, so we add them.
              date_parts = date_val.split('/')
              date_val = "%02d/%02d/%02d" % (int(date_parts[0]), int(date_parts[1]), int(date_parts[2]))
              #If we want to use midnight as the starting time, or we did not have a sample time.
              if start_time_midnight or len(time_val) == 0:
                time_val = '00:00:00'
              #Time doesn't have leading 0's so add them
              else:
                hours_mins = time_val.split(':')
                time_val = "%02d:%02d:00" % (int(hours_mins[0]), int(hours_mins[1]))
              try:
                wq_date = eastern.localize(datetime.strptime('%s %s' % (date_val, time_val), '%m/%d/%y %H:%M:%S'))
              except ValueError, e:
                try:
                  wq_date = eastern.localize(datetime.strptime('%s %s' % (date_val, time_val), '%m/%d/%Y %H:%M:%S'))
                except ValueError, e:
                  if logger:
                    logger.error("Processing halted at line: %d" % (line_num))
                    logger.exception(e)
                  sys.exit(-1)
              #Convert to UTC
              wq_utc_date = wq_date.astimezone(timezone('UTC'))

              if (starting_date is not None and wq_utc_date >= starting_date) or (starting_date is None):
                if fl_sites.get_site(cleaned_site_name):
                  new_outfile = False
                  if current_site != cleaned_site_name:
                    #Initialize site name
                    if current_site != None:
                      site_data_file.close()

                    current_site = cleaned_site_name
                    append_file = False
                    if current_site in processed_sites:
                      if logger:
                        logger.debug("Site: %s has been found again, data is not ordered.")
                      append_file = True
                    else:
                      processed_sites.append(current_site)
                    #We need to create a new data access object using the boundaries for the station.
                    site = fl_sites.get_site(cleaned_site_name)
                    try:
                      #Get the station specific tide stations
                      tide_station = config_file.get(cleaned_site_name, 'tide_station')
                      offset_tide_station = config_file.get(cleaned_site_name, 'offset_tide_station')
                      tide_offset_settings = {
                        'tide_station': config_file.get(offset_tide_station, 'station_id'),
                        'hi_tide_time_offset': config_file.getint(offset_tide_station, 'hi_tide_time_offset'),
                        'lo_tide_time_offset': config_file.getint(offset_tide_station, 'lo_tide_time_offset'),
                        'hi_tide_height_offset': config_file.getfloat(offset_tide_station, 'hi_tide_height_offset'),
                        'lo_tide_height_offset': config_file.getfloat(offset_tide_station, 'lo_tide_height_offset')
                      }

                    except ConfigParser.Error, e:
                      if logger:
                        logger.exception(e)

                    fl_wq_data.reset(site=site,
                                      tide_station=tide_station,
                                      tide_offset_params=tide_offset_settings,
                                      tide_data_obj=tide_file)

                    clean_filename = cleaned_site_name.replace(' ', '_')
                    sample_site_filename = "%s/%s-Historical.csv" % (summary_out_file, clean_filename)
                    write_header = True
                    try:
                      if not append_file:
                        if logger:
                          logger.debug("Opening sample site history file: %s" % (sample_site_filename))
                        site_data_file = open(sample_site_filename, 'w')
                      else:
                        if logger:
                          logger.debug("Opening sample site history file with append: %s" % (sample_site_filename))
                        site_data_file = open(sample_site_filename, 'a')
                        write_header = False
                    except IOError, e:
                      if logger:
                        logger.exception(e)
                      raise e
                  if logger:
                    logger.debug("Start building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))
                  site_data = OrderedDict([('autonumber', row['autonumber']),
                                           ('station_name',row['SPLocation']),
                                           ('sample_datetime', wq_date.strftime("%Y-%m-%d %H:%M:%S")),
                                           ('sample_datetime_utc', wq_utc_date.strftime("%Y-%m-%d %H:%M:%S")),
                                           ('County', row['County']),
                                           ('enterococcus_value', row['enterococcus']),
                                           ('enterococcus_code', row['enterococcus_code'])])
                  try:
                    fl_wq_data.query_data(wq_utc_date, wq_utc_date, site_data)
                  except Exception,e:
                    if logger:
                      logger.exception(e)
                    sys.exit(-1)
                  #wq_data_obj.append(site_data)
                  header_buf = []
                  data = []
                  for key in site_data:
                    if write_header:
                      header_buf.append(key)
                    if site_data[key] != wq_defines.NO_DATA:
                      data.append(str(site_data[key]))
                    else:
                      data.append("")
                  if write_header:
                    site_data_file.write(",".join(header_buf))
                    site_data_file.write('\n')
                    header_buf[:]
                    write_header = False

                  site_data_file.write(",".join(data))
                  site_data_file.write('\n')
                  site_data_file.flush()
                  data[:]
                  if logger:
                    logger.debug("Finished building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))



                else:
                  try:
                    sites_not_found.index(row['SPLocation'])
                  except ValueError,e:
                    sites_not_found.append(row['SPLocation'])

          line_num += 1
      wq_file.close()
      if logger:
        logger.debug("Stations not matching: %s" % (", ".join(sites_not_found)))


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportData", dest="import_data",
                    help="Directory to import XMRG files from" )
  parser.add_option("-b", "--BuildSummaryData",
                    action="store_true", default=True,
                    dest="build_summary_data",
                    help="Flag that specifies to construct summary file.")
  parser.add_option("-w", "--WaterQualityHistoricalFile", dest="historical_wq_file",
                    help="Input file with the dates and stations we are creating summary for." )
  parser.add_option("-r", "--HistoricalSummaryHeaderRow", dest="historical_file_header_row",
                    help="Input file with the dates and stations we are creating summary for." )
  parser.add_option("-s", "--HistoricalSummaryOutPath", dest="summary_out_path",
                    help="Directory to write the historical summary data to." )
  parser.add_option("-d", "--StartDate", dest="starting_date",
                    help="Date to use for the retrieval." )
  parser.add_option("-m", "--StartTimeMidnight", dest="start_time_midnight",
                    action="store_true", default=True,
                    help="Set time to 00:00:00 for the queries instead of the sample time." )
  parser.add_option("-t", "--TideDataFile", dest="tide_data_file",
                    help="If used, this is the path to a tide data csv file.", default=None )


  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.config_file)

    logger = None
    logConfFile = configFile.get('logging', 'xmrg_ingest')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger('florida_wq_processing_logger')
      logger.info("Log file opened.")
  except ConfigParser.Error, e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    starting_date = None
    if options.starting_date:
      starting_date = timezone('UTC').localize(datetime.strptime(options.starting_date, '%Y-%m-%dT%H:%M:%S'))
    if options.build_summary_data:
      create_historical_summary(options.config_file,
                                options.historical_wq_file,
                                options.historical_file_header_row.split(','),
                                options.summary_out_path,
                                starting_date,
                                options.start_time_midnight,
                                options.tide_data_file,
                                True)
  if logger:
    logger.info("Log file closed.")
  return


if __name__ == "__main__":
  main()
