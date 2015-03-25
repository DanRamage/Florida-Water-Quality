import sys
sys.path.append('../commonfiles')

import os
import logging.config
import optparse
import ConfigParser
import csv
from datetime import datetime
from pytz import timezone

#import scipy.io as sio
from netCDF4 import Dataset
from NOAATideData import noaaTideData

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from wqDatabase import wqDB

from date_time_utils import *
from florida_wq_processing import florida_sample_sites

def import_nws_data(nws_directory, xenia_db_name):
  logger = logging.getLogger('import_nws_data_logger')

  nws_file_list = os.listdir(nws_directory)
  xenia_db = wqDB(xenia_db_name, 'import_nws_data_logger')
  header_row = [
  "WBAN",
  "Date",
  "Time",
  "StationType",
  "SkyCondition",
  "SkyConditionFlag",
  "Visibility",
  "VisibilityFlag",
  "WeatherType",
  "WeatherTypeFlag",
  "DryBulbFarenheit",
  "DryBulbFarenheitFlag",
  "DryBulbCelsius",
  "DryBulbCelsiusFlag",
  "WetBulbFarenheit",
  "WetBulbFarenheitFlag",
  "WetBulbCelsius",
  "WetBulbCelsiusFlag",
  "DewPointFarenheit",
  "DewPointFarenheitFlag",
  "DewPointCelsius",
  "DewPointCelsiusFlag",
  "RelativeHumidity",
  "RelativeHumidityFlag",
  "WindSpeed",
  "WindSpeedFlag",
  "WindDirection",
  "WindDirectionFlag",
  "ValueForWindCharacter",
  "ValueForWindCharacterFlag",
  "StationPressure",
  "StationPressureFlag",
  "PressureTendency",
  "PressureTendencyFlag",
  "PressureChange",
  "PressureChangeFlag",
  "SeaLevelPressure",
  "SeaLevelPressureFlag",
  "RecordType",
  "RecordTypeFlag",
  "HourlyPrecip",
  "HourlyPrecipFlag",
  "Altimeter",
  "AltimeterFlag"
  ]
  add_entries = True
  platform_handle = 'nws.ksrq.met'
  if add_entries:
    org_id = xenia_db.organizationExists('nws')
    if org_id == -1:
      org_id =  xenia_db.addOrganization({'short_name': 'nws'})
    #Add the platforms to represent the watersheds and drainage basins
    if xenia_db.platformExists(platform_handle) == -1:
      if xenia_db.addPlatform({'organization_id': org_id,
                                    'platform_handle': platform_handle,
                                    'short_name': 'ksrq',
                                    'active': 1}) == -1:
        if logger:
          logger.error("Failed to add platform: %s for org_id: %d, cannot continue" % (platform_handle, org_id))

  #There is header data above the header line. Start on line 9 for data.
  start_data_line = 7
  sensor_ids = {}
  lat = 27.401
  lon = -82.558
  row_entry_date = datetime.now()
  #Dates in files are LST. WE want to work internally in UTC.
  eastern = timezone('US/Eastern')

  try:
    sensor_id, m_type_id = xenia_db.add_sensor_to_platform(platform_handle, 'air_temperature', 'celsius', 1)
    sensor_ids['air_temperature'] = {'sensor_id': sensor_id, 'm_type_id': m_type_id}

    sensor_id, m_type_id = xenia_db.add_sensor_to_platform(platform_handle, 'wind_speed', 'mph', 1)
    sensor_ids['wind_speed'] = {'sensor_id': sensor_id, 'm_type_id': m_type_id}

    sensor_id, m_type_id = xenia_db.add_sensor_to_platform(platform_handle, 'wind_from_direction', 'degrees_true', 1)
    sensor_ids['wind_from_direction'] = {'sensor_id': sensor_id, 'm_type_id': m_type_id}
  except ValueError, e:
    if logger:
      logger.exception(e)
  else:
    for nws_file_name in nws_file_list:
      line_cnt = 0

      try:
        full_path = "%s%s" % (nws_directory, nws_file_name)
        if logger:
          logger.debug("Opening NWS File: %s" % (full_path))

        with open(full_path,"r") as nws_file:
          dict_file = csv.DictReader(nws_file, delimiter=',', quotechar='"', fieldnames=header_row)
          for row in dict_file:
            if line_cnt > start_data_line:
              if row['Date'] is not None:
                #Format time to have leading 0's for hour and min when needed.
                time_padded = row['Time'].zfill(4)
                try:
                  date_data = eastern.localize(datetime.strptime('%sT%s' % (row['Date'], time_padded), '%Y%m%dT%H%M'))
                  date_data_utc = date_data.astimezone(timezone('UTC'))
                except Exception, e:
                  if logger:
                    logger.exception(e)
                try:
                  air_temp = float(row['DryBulbCelsius'])
                except ValueError, e:
                  air_temp = None
                  if logger:
                    logger.exception(e)
                try:
                  wind_speed = float(row['WindSpeed'])
                  wind_dir = float(row['WindDirection'])
                except ValueError, e:
                  wind_speed = None
                  wind_dir = None
                  if logger:
                    logger.exception(e)
                if logger:
                  logger.debug("Date: %s Air Temp: %s Wind Speed: %s Wind Dir: %s"\
                               % (date_data_utc.strftime('%Y-%m-%d %H:%M:%S'), str(air_temp), str(wind_speed), str(wind_dir)))
                #(self, mTypeID, sensorID, platformHandle, date, lat, lon, z, mValues, sOrder=1, autoCommit=True, rowEntryDate=None):
                if air_temp:
                  if xenia_db.addMeasurementWithMType(
                                                sensor_ids['air_temperature']['m_type_id'],
                                                sensor_ids['air_temperature']['sensor_id'],
                                                platform_handle,
                                                date_data_utc.strftime('%Y-%m-%dT%H:%M:%S'),
                                                lat, lon,
                                                0,
                                                [air_temp],
                                                1,
                                                True,
                                                row_entry_date.strftime('%Y-%m-%d %H:%M:%S')) == False:
                    if logger:
                      logger.error("Unable to add Date: %s air_temperature"\
                                   % (date_data_utc.strftime('%Y-%m-%d %H:%M:%S')))
                if wind_speed and wind_dir:
                  if xenia_db.addMeasurementWithMType(
                                                sensor_ids['wind_speed']['m_type_id'],
                                                sensor_ids['wind_speed']['sensor_id'],
                                                platform_handle,
                                                date_data_utc.strftime('%Y-%m-%dT%H:%M:%S'),
                                                lat, lon,
                                                0,
                                                [wind_speed],
                                                1,
                                                True,
                                                row_entry_date.strftime('%Y-%m-%d %H:%M:%S')) == False:
                    if logger:
                      logger.error("Unable to add Date: %s wind_speed"\
                                   % (date_data_utc.strftime('%Y-%m-%d %H:%M:%S')))
                  if xenia_db.addMeasurementWithMType(
                                                sensor_ids['wind_from_direction']['m_type_id'],
                                                sensor_ids['wind_from_direction']['sensor_id'],
                                                platform_handle,
                                                date_data_utc.strftime('%Y-%m-%dT%H:%M:%S'),
                                                lat, lon,
                                                0,
                                                [wind_dir],
                                                1,
                                                True,
                                                row_entry_date.strftime('%Y-%m-%d %H:%M:%S')) == False:
                    if logger:
                      logger.error("Unable to add Date: %s wind_from_direction"\
                                   % (date_data_utc.strftime('%Y-%m-%d %H:%M:%S')))
              else:
                if logger:
                  logger.error("Line: %d does not have valid data." % (line_cnt))
            line_cnt += 1
      except (IOError,Exception) as e:
        if logger:
          logger.error("Line: %d has error." % (line_cnt))
          logger.exception(e)
      if logger:
        logger.debug("Processed: %d lines." % (line_cnt))
  return
"""
def import_c10_matlab_data(matlab_file, type):#, time_field_name, data_fields):
  if type == 'salinity':
    import_c10_salinity_data(matlab_file)
  elif type == 'water_temperature':
    import_c10_water_temp_data(matlab_file)
"""
"""
def import_c10_salinity_data(matlab_file):
  logger = logging.getLogger('import_c10_matlab_data_logger')

  file_path, ext = os.path.splitext(matlab_file)

  #mat_contents = sio.loadmat('sabgom.mat', struct_as_record=False, squeeze_me=True)
  mat_contents = sio.loadmat(matlab_file)
  if logger:
    logger.debug(mat_contents)
  matlab_time = mat_contents['tt']

  #for data_field in data_fields:

  matlab_s_01 = mat_contents['S_01']
  matlab_s_04 = mat_contents['S_04']
  matlab_s_10 = mat_contents['S_10']
  matlab_s_19 = mat_contents['S_19']

  time = matlab_time[:].flatten()
  s_01 = matlab_s_01[:].flatten()
  s_04 = matlab_s_04[:].flatten()
  s_10 = matlab_s_10[:].flatten()
  s_19 = matlab_s_19[:].flatten()
  if logger:
    logger.debug("Array lengths: Time: %d S_01: %d S_04: %d S_10: %d S_19: %d"\
      % (len(time), len(s_01), len(s_04), len(s_10), len(s_19)))

  #Let convert from the matlab date number into a unix epoch value.
  DAY = 24*60*60 # POSIX day in seconds (exact value)
  the_unix_epoch = datetime(1970,1,1,0,0,0,0,timezone('UTC'))
  matlab_unix_epoch_time = datetime2matlabdn(the_unix_epoch)
  unix_epoch_time = [((matlab_time - matlab_unix_epoch_time) * DAY) for matlab_time in time]

  root_grp = Dataset(("%s.nc" % (file_path)), 'w', format='NETCDF4')
  root_grp.description = 'Salinity at C10 at 1m, 4m, 10m, 19m depths'

  # dimensions
  root_grp.createDimension('time', None)

  # variables
  times = root_grp.createVariable('time', 'f8', ('time',))
  times.standard_name = 'time'
  times.units = "seconds since 1970-01-01 00:00:00 UTC"
  times.axis = "T"

  nc_S01 = root_grp.createVariable('salinity_01', 'f4', ('time'))
  nc_S01.standard_name = 'sea_water_salinity'
  nc_S01.units = "PSU"
  nc_S01.depth = "1"


  nc_S04 = root_grp.createVariable('salinity_04', 'f4', ('time'))
  nc_S04.standard_name = 'sea_water_salinity'
  nc_S04.units = "PSU"
  nc_S04.depth = "4"

  nc_S10 = root_grp.createVariable('salinity_10', 'f4', ('time'))
  nc_S10.standard_name = 'sea_water_salinity'
  nc_S10.units = "PSU"
  nc_S10.depth = "10"

  nc_S19 = root_grp.createVariable('salinity_19', 'f4', ('time'))
  nc_S19.standard_name = 'sea_water_salinity'
  nc_S19.units = "PSU"
  nc_S19.depth = "19"


  # data
  times[:] = unix_epoch_time

  nc_S01[:] = s_01[:]
  nc_S04[:] = s_04[:]
  nc_S10[:] = s_10[:]
  nc_S19[:] = s_19[:]

  root_grp.close()

  return
"""
def import_c10_matlab_data(matlab_file):
  logger = logging.getLogger('import_c10_matlab_data_logger')

  files = matlab_file.split(',')

  file_path, ext = os.path.split(files[0])

  #mat_contents = sio.loadmat('sabgom.mat', struct_as_record=False, squeeze_me=True)

  temp_contents= sio.loadmat(files[0])
  sal_contents = sio.loadmat(files[1])
  if logger:
    logger.debug(temp_contents)
    logger.debug(sal_contents)

  sal_time = sal_contents['tt']
  temp_time = temp_contents['tt']
  if len(sal_time) == len(temp_time):
    for ndx in range(0, len(sal_time)):
      if sal_time[ndx] != temp_time[ndx]:
        if logger:
          logger.error("Times do not match at ndx: %d Sal: %f Temp: %f" % (ndx, len(sal_time), len(temp_time)))
          return
  #for data_field in data_fields:
  matlab_s_01 = sal_contents['S_01']
  matlab_s_04 = sal_contents['S_04']
  matlab_s_10 = sal_contents['S_10']
  matlab_s_19 = sal_contents['S_19']

  time = sal_time[:].flatten()
  s_01 = matlab_s_01[:].flatten()
  s_04 = matlab_s_04[:].flatten()
  s_10 = matlab_s_10[:].flatten()
  s_19 = matlab_s_19[:].flatten()
  if logger:
    logger.debug("Array lengths: Time: %d S_01: %d S_04: %d S_10: %d S_19: %d"\
      % (len(time), len(s_01), len(s_04), len(s_10), len(s_19)))

  matlab_t_01 = temp_contents['T_01']
  matlab_t_04 = temp_contents['T_04']
  matlab_t_10 = temp_contents['T_10']
  matlab_t_19 = temp_contents['T_19']

  t_01 = matlab_t_01[:].flatten()
  t_04 = matlab_t_04[:].flatten()
  t_10 = matlab_t_10[:].flatten()
  t_19 = matlab_t_19[:].flatten()

  if logger:
    logger.debug("Array lengths: T_01: %d T_04: %d T_10: %d T_19: %d"\
      % ( len(t_01), len(t_04), len(t_10), len(t_19)))

  #Let convert from the matlab date number into a unix epoch value.
  DAY = 24*60*60 # POSIX day in seconds (exact value)
  the_unix_epoch = datetime(1970,1,1,0,0,0,0,timezone('UTC'))
  matlab_unix_epoch_time = datetime2matlabdn(the_unix_epoch)
  unix_epoch_time = [((matlab_time - matlab_unix_epoch_time) * DAY) for matlab_time in time]

  root_grp = Dataset(("%s/c10_salinity_water_temp.nc" % (file_path)), 'w', format='NETCDF4')
  root_grp.description = 'QAQCd historical data for Salinity and Water Temperature at C10 at 1m, 4m, 10m, 19m depths'

  # dimensions
  root_grp.createDimension('time', None)
  # dimensions
  times = root_grp.createVariable('time', 'f8', ('time',))
  times.standard_name = 'time'
  times.units = "seconds since 1970-01-01 00:00:00 UTC"
  times.axis = "T"

  # variables
  nc_S01 = root_grp.createVariable('salinity_01', 'f4', ('time'))
  nc_S01.standard_name = 'sea_water_salinity'
  nc_S01.units = "PSU"
  nc_S01.depth = "1"


  nc_S04 = root_grp.createVariable('salinity_04', 'f4', ('time'))
  nc_S04.standard_name = 'sea_water_salinity'
  nc_S04.units = "PSU"
  nc_S04.depth = "4"

  nc_S10 = root_grp.createVariable('salinity_10', 'f4', ('time'))
  nc_S10.standard_name = 'sea_water_salinity'
  nc_S10.units = "PSU"
  nc_S10.depth = "10"

  nc_S19 = root_grp.createVariable('salinity_19', 'f4', ('time'))
  nc_S19.standard_name = 'sea_water_salinity'
  nc_S19.units = "PSU"
  nc_S19.depth = "19"

  nc_T01 = root_grp.createVariable('temperature_01', 'f4', ('time'))
  nc_T01.standard_name = 'water_temperature'
  nc_T01.units = "celsius"
  nc_T01.depth = "1"


  nc_T04 = root_grp.createVariable('temperature_04', 'f4', ('time'))
  nc_T04.standard_name = 'water_temperature'
  nc_T04.units = "celsius"
  nc_T04.depth = "4"

  nc_T10 = root_grp.createVariable('temperature_10', 'f4', ('time'))
  nc_T10.standard_name = 'water_temperature'
  nc_T10.units = "celsius"
  nc_T10.depth = "10"

  nc_T19 = root_grp.createVariable('temperature_19', 'f4', ('time'))
  nc_T19.standard_name = 'water_temperature'
  nc_T19.units = "celsius"
  nc_T19.depth = "19"


  # data
  times[:] = unix_epoch_time

  nc_S01[:] = s_01[:]
  nc_S04[:] = s_04[:]
  nc_S10[:] = s_10[:]
  nc_S19[:] = s_19[:]

  nc_T01[:] = t_01[:]
  nc_T04[:] = t_04[:]
  nc_T10[:] = t_10[:]
  nc_T19[:] = t_19[:]

  plt_times = [t - 1 for t in time] #subtract one day since netcdf doesn't understand 0000-00-00 only 0000-01-01

  plt.plot_date(plt_times,nc_S04[:])

  plt.xlabel(times.standard_name + ' (' + times.units + ')')
  plt.ylabel(nc_S04.standard_name   + ' (' + nc_S04.units   + ')')
  plt.title('salinity_C10_04')

  #plt.show()
  plt.savefig(("%s/c10_salinity.png" % (file_path)))
  root_grp.close()

  return

def import_tide_data(config_file, output_file):
  logger = logging.getLogger('import_nws_data_logger')

  configFile = ConfigParser.RawConfigParser()
  configFile.read(config_file)

  wq_sample_data_file = configFile.get("wq_sample_data", "file")
  wq_sample_data_file_header = configFile.get("wq_sample_data", "file_header").split(',')
  sites_location_file = configFile.get("boundaries_settings", "sample_sites")
  boundaries_location_file = configFile.get("boundaries_settings", "boundaries_file")
  tide_station = configFile.get("tide_station", "station_id")
  try:
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
    with open(output_file, "w") as tide_data_file:
      tide_data_file.write("Station,Date,Range,Hi,Lo\n")

      with open(wq_sample_data_file, "rU") as wq_file:
        #Dates in the spreadsheet are stored in EST. WE want to work internally in UTC.
        eastern = timezone('US/Eastern')

        wq_history_file = csv.DictReader(wq_file, delimiter=',', quotechar='"', fieldnames=wq_sample_data_file_header)
        line_num = 0
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
              tide = noaaTideData(use_raw=True, logger=logger)
              #Date/Time format for the NOAA is YYYYMMDD

              if logger:
                logger.debug("Start retrieving tide data for station: %s date: %s" % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))

              tide_data = tide.calcTideRange(beginDate = wq_utc_date.strftime('%Y%m%d'),
                                 endDate = wq_utc_date.strftime('%Y%m%d'),
                                 station=tide_station,
                                 datum='MLLW',
                                 units='feet',
                                 timezone='GMT',
                                 smoothData=False)
              if tide_data and tide_data['HH'] is not None and tide_data['LL'] is not None:
                try:
                  range = tide_data['HH']['value'] - tide_data['LL']['value']
                except TypeError, e:
                  if logger:
                    logger.exception(e)
                else:
                  #Save tide station values.
                  wq_range = range
                  tide_hi = tide_data['HH']['value']
                  tide_lo = tide_data['LL']['value']
                  tide_data_file.write("%s,%s,%f,%f,%f\n"\
                                       % (tide_station,wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S"),range,tide_hi,tide_lo))
              else:
                if logger:
                  logger.error("Tide data for station: %s date: %s not available or only partial." % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))

              if logger:
                logger.debug("Finished retrieving tide data for station: %s date: %s" % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))

          line_num += 1
  except IOError, e:
    if logger:
      logger.exception(e)


  return

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-n", "--NWSDirectory", dest="nws_directory",
                    help="" )
  parser.add_option("-m", "--MatlabFile", dest="matlab_file",
                    help="" )
  parser.add_option("-d", "--MatlabDataType", dest="matlab_data_type",
                    help="" )
  parser.add_option("-t", "--ImportTideData", dest="ImportTideData",
                    action="store_true", default=False,
                    help="" )
  parser.add_option("-f", "--TideDataFile", dest="tide_data_file",
                    help="" )


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
    xenia_db = configFile.get('database', 'name')
  except ConfigParser.Error, e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    if options.ImportTideData and len(options.tide_data_file):
      import_tide_data(options.config_file, options.tide_data_file)

    if options.nws_directory is not None and len(options.nws_directory):
      import_nws_data(options.nws_directory, xenia_db)

    if options.matlab_file is not None and len(options.matlab_file):
      import_c10_matlab_data(options.matlab_file)



  if logger:
    logger.info("Log file closed.")
  return


if __name__ == "__main__":
  main()
