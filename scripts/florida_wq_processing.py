import sys
sys.path.append('../commonfiles')

import logging.config
from datetime import datetime, timedelta
import optparse
import ConfigParser
import csv
from pytz import timezone
import netCDF4 as nc
from suds import WebFault
from suds.client import Client

from bisect import bisect_left

from collections import OrderedDict

from wqHistoricalData import wq_data
from wqXMRGProcessing import wqDB
from wqHistoricalData import station_geometry, item_geometry, sampling_sites, wq_defines, geometry_list

"""
florida_wq_data
Class is responsible for retrieving the data used for the sample sites models.
"""
class florida_wq_data(wq_data):
  """
  Function: __init__
  Purpose: Initializes the class.
  Parameters:
    boundaries - The boundaries for the NEXRAD data the site falls within, this is required.
    xenia_database_name - The full file path to the xenia database that houses the NEXRAD and other
      data we use in the models. This is required.
  """
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)
    self.database_name = kwargs['xenia_database_name']

    #Connect to netcdf file for retrieving data from c10 buoy. To speed up retrieval, we connect
    #only once and retrieve the times.
    c10_url = 'http://tds.secoora.org/thredds/dodsC/c10_salinity_water_temp.nc'
    self.ncObj = nc.Dataset(c10_url)
    self.c10_times = self.ncObj.variables['time'][:]
    self.c10_water_temp = self.ncObj.variables['temperature_04'][:]
    self.c10_salinity = self.ncObj.variables['salinity_04'][:]

  def initialize(self, **kwargs):
    self.boundaries = kwargs['boundaries']


  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """
  def query_data(self, start_date, end_date, wq_tests_data):
    #dt_time = [datetime.fromtimestamp(t) for t in c10_time]
    #strip timezone info out.
    start_date_no_tz = start_date.replace(tzinfo=None)
    #self.get_nws_data(start_date_no_tz, wq_tests_data)
    self.get_thredds_data(start_date_no_tz, wq_tests_data)
    #self.get_nexrad_data(start_date, wq_tests_data)

  def get_nws_data(self, start_date, wq_tests_data):
    logging.getLogger('suds.client').setLevel(logging.DEBUG)


    wq_tests_data['nws_ksrq_avg_wspd'] = wq_defines.NO_DATA
    wq_tests_data['nws_ksrq_avg_wdir'] = wq_defines.NO_DATA

    url = 'http://www.weather.gov/forecasts/xml/DWMLgen/wsdl/ndfdXML.wsdl'
    client = Client(url)
    begin_date = start_date - timedelta(hours=24)
    begin_date = datetime.strptime('2004-05-01T00:00:00', '%Y-%m-%dT%H:%M:%S')
    start = begin_date.strftime('%Y-%m-%dT%H:%M:%S')
    end = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    if self.logger:
      self.logger.debug("NWS KSRQ Query for: %s to %s" % (start, end))

    try:
      data = client.service.NDFDgen(latitude=27.4, longitude=-82.57,
                            product='time-series',
                            start_time=start, endTime=end)
    except WebFault, e:
      if self.logger:
        self.logger.exception(e)
    else:
      print data
    return

  def get_nexrad_data(self, start_date, wq_tests_data):
    xenia_db = wqDB(self.database_name, type(self).__name__)
    #Collect the radar data for the boundaries.
    for boundary in self.boundaries:
      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 192, 24):
        var_name = '%s_summary_%d' % (boundary.name.lower().replace(' ', '_'), prev_hours)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        radar_val = xenia_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                    start_date,
                                                                    prev_hours,
                                                                    'precipitation_radar_weighted_average',
                                                                    'mm')
        if radar_val != None:
          wq_tests_data[var_name] = radar_val
        else:
          if self.logger:
            self.logger.error("No data available for boundary: %s Date: %s. Error: %s" %(var_name, start_date, xenia_db.getErrorInfo()))
      var_name = '%s_dry_days_count' % (boundary.name.lower().replace(' ', '_'))
      wq_tests_data[var_name] = wq_defines.NO_DATA
      prev_dry_days = xenia_db.getPrecedingRadarDryDaysCount(platform_handle,
                                             start_date,
                                             'precipitation_radar_weighted_average',
                                             'mm')
      if prev_dry_days is not None:
        wq_tests_data[var_name] = prev_dry_days

      var_name = '%s_rainfall_intesity' % (boundary.name.lower().replace(' ', '_'))
      wq_tests_data[var_name] = wq_defines.NO_DATA
      rainfall_intensity = xenia_db.calcRadarRainfallIntensity(platform_handle,
                                                               start_date,
                                                               60,
                                                              'precipitation_radar_weighted_average',
                                                              'mm')
      if rainfall_intensity is not None:
        wq_tests_data[var_name] = rainfall_intensity
    xenia_db.DB.close()

  def get_thredds_data(self, start_date, wq_tests_data):
    start_epoch_time = float(start_date.strftime('%s'))
    #Find the starting time index to work from.
    if self.logger:
      self.logger.debug("Thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    salinity_var_name = 'c10_avg_salinity_%d' % (24)
    wq_tests_data[salinity_var_name] = wq_defines.NO_DATA
    wq_tests_data['c10_min_salinity'] = wq_defines.NO_DATA
    wq_tests_data['c10_max_salinity'] = wq_defines.NO_DATA
    water_temp_var_name = 'c10_avg_water_temp_%d' % (24)
    wq_tests_data[water_temp_var_name] = wq_defines.NO_DATA
    wq_tests_data['c10_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['c10_max_water_temp'] = wq_defines.NO_DATA

    closest_start_time_idx = bisect_left(self.c10_times, start_epoch_time)


    if (closest_start_time_idx != 0 and closest_start_time_idx != len(self.c10_times)) \
      and (closest_start_time_idx != len(self.c10_times)):

      closest_datetime = datetime.fromtimestamp(self.c10_times[closest_start_time_idx])
      prev_hour_dt = closest_datetime - timedelta(hours=24)
      prev_hour_epoch = float(prev_hour_dt.strftime('%s'))
      #Get closest index for date/time for our prev_hour interval.
      closest_prev_hour_time_idx = bisect_left(self.c10_times, prev_hour_epoch)

      if self.logger:
        self.logger.debug("Thredds C10 found closest datetime: %s" % (closest_datetime.strftime('%Y-%m-%d %H:%M:%S')))
      #for prev_hours in range(24, 24, 24):

      rec_cnt = closest_start_time_idx-closest_prev_hour_time_idx
      avg_c10_salinity = 0
      min_sal = None
      max_sal = None
      avg_c10_wt = 0
      min_temp = None
      max_temp = None
      for ndx in range(closest_prev_hour_time_idx, closest_start_time_idx):
        avg_c10_salinity += self.c10_salinity[ndx]
        if min_sal is None or min_sal > self.c10_salinity[ndx]:
          min_sal = self.c10_salinity[ndx]
        if max_sal is None or max_sal < self.c10_salinity[ndx]:
          max_sal = self.c10_salinity[ndx]

        avg_c10_wt += self.c10_water_temp[ndx]
        if min_temp is None or min_temp > self.c10_water_temp[ndx]:
          min_temp = self.c10_water_temp[ndx]
        if max_sal is None or max_temp < self.c10_water_temp[ndx]:
          max_temp = self.c10_water_temp[ndx]
      if rec_cnt > 0:
        avg_c10_salinity = avg_c10_salinity / rec_cnt
        avg_c10_wt = avg_c10_wt / rec_cnt
        wq_tests_data['c10_avg_salinity_24'] = avg_c10_salinity
        wq_tests_data['c10_avg_water_temp_24'] = avg_c10_wt

        wq_tests_data['c10_min_salinity'] = min_sal
        wq_tests_data['c10_max_salinity'] = max_sal
        wq_tests_data['c10_min_water_temp'] = min_temp
        wq_tests_data['c10_max_water_temp'] = max_temp

        if self.logger:
          self.logger.debug("Thredds C10 Start: %s End: %s Avg Salinity: %f (%f,%f) Avg Water Temp: %f (%f,%f)"\
                            % (start_date.strftime('%Y-%m-%d %H:%M:%S'), prev_hour_dt.strftime('%Y-%m-%d %H:%M:%S'),
                               avg_c10_salinity, min_sal, max_sal, avg_c10_wt, min_temp, max_temp))
    return

"""
florida_sample_sites
Overrides the default sampling_sites object so we can load the sites from the florida data.
"""
class florida_sample_sites(sampling_sites):
  def __init__(self, use_logger=False):
    self.logger = None
    if use_logger:
      self.logger = logging.getLogger(type(self).__name__)

  """
  Function: load_sites
  Purpose: Given the file_name in the kwargs, this will read the file and load up the sampling
    sites we are working with.
  Parameters:
    **kwargs - Must have file_name which is full path to the sampling sites csv file.
  Return:
    True if successfully loaded, otherwise False.
  """
  def load_sites(self, **kwargs):
    if 'file_name' in kwargs:
      if 'boundary_file' in kwargs:
        fl_boundaries = geometry_list(use_logger=True)
        fl_boundaries.load(kwargs['boundary_file'])

      try:
        header_row = ["WKT","EPAbeachID","SPLocation","Boundary"]
        if self.logger:
          self.logger.debug("Reading sample sites file: %s" % (kwargs['file_name']))

        sites_file = open(kwargs['file_name'], "rU")
        dict_file = csv.DictReader(sites_file, delimiter=',', quotechar='"', fieldnames=header_row)
      except IOError, e:
        if self.logger:
          self.logger.exception(e)
      else:
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            add_site = False
            #The site could be in multiple boundaries, so let's search to see if it is.
            station = self.get_site(row['SPLocation'])
            if station is None:
              add_site = True
              station = station_geometry(row['SPLocation'], None)
              self.append(station)

              boundaries = row['Boundary'].split(',')
              for boundary in boundaries:
                boundary_geometry = fl_boundaries.get_geometry_item(boundary)
                if add_site:
                  #Add the containing boundary
                  station.contained_by.append(boundary_geometry)
          line_num += 1

    return False


def create_historical_summary(config_file,
                              historical_wq_file,
                              header_row,
                              summary_out_file,
                              use_logger=False):
  logger = None
  try:
    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    xenia_db_file = config_file.get('database', 'name')
  except ConfigParser, e:
    if logger:
      logger.exception(e)
  else:
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
    if use_logger:
      logger = logging.getLogger('create_historical_summary_logger')
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
      wq_data_obj = []
      current_site = None

      stop_date = eastern.localize(datetime.strptime('2014-01-29 00:00:00', '%Y-%m-%d %H:%M:%S'))
      stop_date = stop_date.astimezone(timezone('UTC'))

      fl_wq_data = florida_wq_data(xenia_database_name=xenia_db_file, use_logger=True)

      for row in wq_history_file:
        #Check to see if the site is one we are using
        if line_num > 0:
          cleaned_site_name = row['SPLocation'].replace("  ", " ")

          if fl_sites.get_site(cleaned_site_name):
            new_outfile = False
            if current_site != cleaned_site_name:
              #Initialize site name
              if current_site != None:
                site_data_file.close()

              current_site = cleaned_site_name

              #We need to create a new data access object using the boundaries for the station.
              site = fl_sites.get_site(cleaned_site_name)

              fl_wq_data.initialize(boundaries=site.contained_by)

              clean_filename = cleaned_site_name.replace(' ', '_')
              sample_site_filename = "%s/%s-Historical.csv" % (summary_out_file, clean_filename)
              write_header = True
              if logger:
                logger.debug("Opening sample site history file: %s" % (sample_site_filename))
              try:
                site_data_file = open(sample_site_filename, 'w')
              except IOError, e:
                if logger:
                  logger.exception(e)
                pass

            date_val = row['Date']
            time_val = row['SampleTime']
            if len(date_val):
              #Date does not have leading 0s sometimes, so we add them.
              date_parts = date_val.split('/')
              date_val = "%02d/%02d/%02d" % (int(date_parts[0]), int(date_parts[1]), int(date_parts[2]))
              if len(time_val) == 0:
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
              if logger:
                logger.debug("Building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))
              site_data = OrderedDict([('autonumber', row['autonumber']),
                                       ('station_name',row['SPLocation']),
                                       ('sample_datetime', wq_utc_date),
                                       ('County', row['County']),
                                       ('enterococcus_value', row['enterococcus']),
                                       ('enterococcus_code', row['enterococcus_code'])])

              fl_wq_data.query_data(site_data['sample_datetime'], site_data['sample_datetime'], site_data)

              #wq_data_obj.append(site_data)
              header_buf = []
              data = []
              for key in site_data:
                if write_header:
                  header_buf.append(key)
                data.append(str(site_data[key]))
              if write_header:
                site_data_file.write(",".join(header_buf))
                site_data_file.write('\n')
                header_buf[:]
                write_header = False

              site_data_file.write(",".join(data))
              site_data_file.write('\n')
              data[:]



          else:
            try:
              sites_not_found.index(row['SPLocation'])
            except ValueError,e:
              sites_not_found.append(row['SPLocation'])

        line_num += 1
      wq_file.close()
      if logger:
        logger.debug("Stations not matching: %s" % (", ".join(sites_not_found)))

  return

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
    if options.build_summary_data:
      create_historical_summary(configFile,
                                options.historical_wq_file,
                                options.historical_file_header_row.split(','),
                                options.summary_out_path,
                                True)
  if logger:
    logger.info("Log file closed.")
  return


if __name__ == "__main__":
  main()
