import sys
sys.path.append('../commonfiles')

import logging.config
from datetime import datetime, timedelta
from pytz import timezone
import traceback

import optparse
import ConfigParser
from collections import OrderedDict
from wq_prediction_tests import wqEquations
from enterococcus_wq_test import EnterococcusPredictionTest

from florida_wq_data import florida_wq_model_data, florida_sample_sites

def build_test_objects(config_file, site_name):
  logger = logging.getLogger('build_test_objects_logger')

  model_list = []
  #Get the sites test configuration ini, then build the test objects.
  try:
    test_config_file = config_file.get(site_name, 'prediction_config')
  except ConfigParser.Error, e:
    if logger:
      logger.exception(e)
  else:
    if logger:
      logger.debug("Site: %s Model Config File: %s" % (site_name, test_config_file))

    model_config_file = ConfigParser.RawConfigParser()
    model_config_file.read(test_config_file)
    #Get the number of prediction models we use for the site.
    model_count = model_config_file.getint("settings", "model_count")
    if logger:
      logger.debug("Site: %s Model count: %d" % (site_name, model_count))

    for cnt in range(model_count):
      model_name = model_config_file.get("model_%d" % (cnt+1), "name")
      model_equation = model_config_file.get("model_%d" % (cnt+1), "formula")
      if logger:
        logger.debug("Site: %s Model name: %s equation: %s" % (site_name, model_name, model_equation))

      test_obj = EnterococcusPredictionTest(model_equation, site_name, model_name)
      model_list.append(test_obj)

  return model_list

def run_wq_models(**kwargs):
  logger = logging.getLogger('run_wq_models_logger')

  config_file = kwargs['config_file']
  try:
    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    xenia_wq_db_file = config_file.get('database', 'name')
    c_10_tds_url = config_file.get('c10_data', 'historical_qaqc_thredds')
    hycom_model_tds_url = config_file.get('hycom_model_data', 'thredds_url')
    model_bbox = config_file.get('hycom_model_data', 'bbox').split(',')
    poly_parts = config_file.get('hycom_model_data', 'within_polygon').split(';')
    model_within_polygon = [(float(lon_lat.split(',')[0]), float(lon_lat.split(',')[1])) for lon_lat in poly_parts]
    xenia_obs_db_host = config_file.get('xenia_observation_database', 'host')
    xenia_obs_db_user = config_file.get('xenia_observation_database', 'user')
    xenia_obs_db_password = config_file.get('xenia_observation_database', 'password')
    xenia_obs_db_name = config_file.get('xenia_observation_database', 'database')
  except ConfigParser.Error, e:
    if logger:
      logger.exception(e)
  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
    #Retrieve the data needed for the models.

    fl_wq_data = florida_wq_model_data(xenia_database_name=xenia_wq_db_file,
                                  c_10_tds_url=c_10_tds_url,
                                  hycom_model_tds_url=hycom_model_tds_url,
                                  model_bbox=model_bbox,
                                  model_within_polygon=model_within_polygon,
                                  use_logger=True,
                                  xenia_obs_db_type='postgres',
                                  xenia_obs_db_host=xenia_obs_db_host,
                                  xenia_obs_db_user=xenia_obs_db_user,
                                  xenia_obs_db_password=xenia_obs_db_password,
                                  xenia_obs_db_name=xenia_obs_db_name)

    site_model_ensemble = []
    for site in fl_sites:
      try:
        #Get all the models used for the particular sample site.
        model_list = build_test_objects(config_file, site.name)
        #Create the container for all the models.
        site_equations = wqEquations(site.name, model_list, True)
        site_model_ensemble.append(site_equations)

        #Get the station specific tide stations
        tide_station = config_file.get(site.name, 'tide_station')
        offset_tide_station = config_file.get(site.name, 'offset_tide_station')
        #We use the virtual tide sites as there no stations near the sites.
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
      else:
        fl_wq_data.reset(site=site,
                          tide_station=tide_station,
                          tide_offset_params=tide_offset_settings)
        site_data = OrderedDict([('station_name',site.name)])
        try:
          fl_wq_data.query_data(kwargs['begin_date'], kwargs['begin_date'], site_data)
        except Exception,e:
          if logger:
            logger.exception(e)


  return

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-s", "--StartDateTime", dest="startDateTime",
                    help="A date to re-run the predictions for, if not provided, the default is the current day. Format is YYYY-MM-DD HH:MM:SS." )

  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logger = None
    logConfFile = config_file.get('logging', 'prediction_engine')
    if logConfFile:
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger('florida_wq_predicition_logger')
      logger.info("Log file opened.")

  except ConfigParser.Error, e:
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    if(options.startDateTime != None):
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      eastern = timezone('US/Eastern')
      est = eastern.localize(datetime.strptime(options.startDateTime, "%Y-%m-%d %H:%M:%S"))
      est = est - timedelta(days=1)
      #Convert to UTC
      begin_date = est.astimezone(timezone('UTC'))
      end_date = begin_date + timedelta(hours=24)
    else:
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      est = datetime.now(timezone('US/Eastern'))
      est = est.replace(hour=0, minute=0, second=0,microsecond=0)
      #Convert to UTC
      begin_date = est.astimezone(timezone('UTC'))

    try:
      run_wq_models(begin_date=begin_date,
                    config_file=config_file)
    except Exception, e:
      logger.exception(e)

  if logger:
    logger.info("Log file closed.")

  return

if __name__ == "__main__":
  main()
