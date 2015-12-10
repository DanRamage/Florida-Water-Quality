import sys
sys.path.append('../commonfiles/python')
import os
import logging.config

from openpyxl import load_workbook
import poplib
import email
import optparse
import ConfigParser
import simplejson as json
import datetime

from florida_wq_data import florida_sample_sites

def contains(list, filter):
  for x in list:
    if filter(x):
      return True
  return False

def check_email_for_update(config_file, use_logging):
  file_list = []
  logger = None
  if use_logging:
    logger = logging.getLogger('check_email_for_update_logger')
    logger.debug("Starting check_email_for_update")
  try:
    email_ini_config_filename = config_file.get("email_settings", "settings_ini")
    email_ini_config_file = ConfigParser.RawConfigParser()
    email_ini_config_file.read(email_ini_config_filename)

    email_host = email_ini_config_file.get("email_settings", "host")
    email_user = email_ini_config_file.get("email_settings", "user")
    email_password = email_ini_config_file.get("email_settings", "password")
    destination_directory = config_file.get("worksheet_settings", "destination_directory")
  except ConfigParser.Error, e:
    if logger:
      logger.exception(e)
  try:
    if logger:
      logger.info("Attempting to connect to email server.")

    pop3_obj = poplib.POP3_SSL(email_host, 995)
    pop3_obj.user(email_user)
    pop3_obj.pass_(email_password)

    if logger:
      logger.info("Successfully connected to email server.")
  except (poplib.error_proto, Exception) as e:
    if logger:
      logger.exception(e)
  else:
    #messages = {}
    #for i in range(1, len(pop3_obj.list()[1]) + 1):
    ##  resp, message, byteCnt = pop3_obj.retr(i)
    #  messages[i] = message
    # messages processing
    emails, total_bytes = pop3_obj.stat()
    for i in range(emails):
        # return in format: (response, ['line', ...], octets)
        response = pop3_obj.retr(i+1)
        raw_message = response[1]

        str_message = email.message_from_string("\n".join(raw_message))

        # save attach
        for part in str_message.walk():
          if logger:
            logger.debug("Content type: %s" % (part.get_content_type()))

          if part.get_content_maintype() == 'multipart':
            continue

          if part.get('Content-Disposition') is None:
            if logger:
              logger.debug("No content disposition")
            continue

          filename = part.get_filename()
          if filename.find('xlsx') != -1:
            if logger:
              logger.debug("Attached filename: %s" % (filename))

            saved_file_name = os.path.join(destination_directory, filename)
            with open(saved_file_name, 'wb') as out_file:
              out_file.write(part.get_payload(decode=1))
              out_file.close()
              file_list.append(saved_file_name)
            #parse_sheet_data(saved_file_name, use_logging)

    #pop3_obj.quit()

  if logger:
    logger.debug("Finished check_email_for_update")
  return file_list

def parse_sheet_data(xl_file_name, use_logging):
  logger = None
  if use_logging:
    logger = logging.getLogger('check_email_for_update_logger')
    logger.debug("Starting parse_sheet_data, parsing file: %s" % (xl_file_name))


  sample_data = {}
  wb = load_workbook(filename = xl_file_name)
  bacteria_data_sheet = wb['SCHD Bact Results']
  date_column = 'A'
  time_column = 'B'
  station_column = 'D'
  entero_column = 'H'
  sample_date = None
  for row_num in range(2,18):
    cell_name = "%s%d" % (station_column, row_num)
    cell_value = "%s%d" % (entero_column, row_num)
    cell_date = "%s%d" % (date_column, row_num)
    cell_time = "%s%d" % (time_column, row_num)
    if isinstance(bacteria_data_sheet[cell_time].value, datetime.time):
      sample_date_time = datetime.datetime.combine(bacteria_data_sheet[cell_date].value, bacteria_data_sheet[cell_time].value)
    else:
      sample_date_time = bacteria_data_sheet[cell_date].value
    sample_data[bacteria_data_sheet[cell_name].value.strip().lower()] = {
      'sample_date': sample_date_time,
      'value': bacteria_data_sheet[cell_value].value
    }
    if sample_date is None:
      sample_date = bacteria_data_sheet[cell_date].value
  if logger:
    logger.debug("Finished parse_sheet_data")

  return sample_data, sample_date

def build_feature(site, sample_date, values, logger):
  if logger:
    logger.debug("Adding feature site: %s Desc: %s" % (site.name, site.description))
  feature = {
    'type': 'Feature',
    'geometry': {
      'type': 'Point',
      'coordinates': [site.object_geometry.x, site.object_geometry.y]
    },
    'properties' : {
      'locale': site.description,
      'sign': False,
      'station': site.name,
      'epaid': site.epa_id,
      'beach': site.county,
      'desc': site.description,
      'len': '',
      'test': {
        'beachadvisories': {
          'date': sample_date,
          'station': site.name,
          'value': values
        }
      }
    }
  }
  return feature

def build_current_file(bacteria_data, config_file, fl_sites, build_missing, use_logging):
  logger = None
  if use_logging:
    logger = logging.getLogger('build_predictions_file_logger')
    logger.debug("Starting build_predictions_file")

  try:
    advisory_results_filename = config_file.get('json_settings', 'advisory_results')
    station_results_directory = config_file.get('json_settings', 'station_results_directory')

  except ConfigParser.Error, e:
    if logger:
      logger.exception(e)
  else:
    features = []
    #Let's go site by site and find the data in the worksheet. Currently we only get the
    #data for Sarasota county.
    values = []
    for site in fl_sites:
      if logger:
        logger.debug("Site: %s Desc: %s searching data" % (site.name, site.description))
      values = None
      if site.description.lower() in bacteria_data:
        if logger:
          logger.debug("Adding feature site: %s Desc: %s" % (site.name, site.description))
        #Build the json structure the web app is going to use.
        station_data = bacteria_data[site.description.lower()]
        if isinstance(station_data['value'], int) or isinstance(station_data['value'], long):
          values = station_data['value']
        else:
          values = station_data['value'].split(';')
          values = [int(val.strip()) for val in values]
      elif build_missing:
        values = []

      if values is not None:
        feature = build_feature(site, station_data['sample_date'].strftime('%Y-%m-%d %H:%M:%S'), values, logger)
        features.append(feature)

        """
        feature = {
          'type': 'Feature',
          'geometry': {
            'type': 'Point',
            'coordinates': [site.object_geometry.x, site.object_geometry.y]
          },
          'properties' : {
            'locale': site.description,
            'sign': False,
            'station': site.name,
            'epaid': site.epa_id,
            'beach': site.county,
            'desc': site.description,
            'len': '',
            'test': {
              'beachadvisories': {
                'date': station_data['sample_date'].strftime('%Y-%m-%d %H:%M:%S'),
                'station': site.name,
                'value': values
              }
            }
          }
        }
        """

    try:
      with open(advisory_results_filename, "w") as json_out_file:
        #Now output JSON file.
        json_data = {
          'type': 'FeatureCollection',
          'features': features
        }
        json_out_file.write(json.dumps(json_data, sort_keys=True))
    except (json.JSONDecodeError, IOError) as e:
      if logger:
        logger.exception(e)

  if logger:
    logger.debug("Finished build_predictions_file")

  return

def build_station_file(bacteria_data, config_file, fl_sites, build_missing, use_logging):
  logger = None
  if use_logging:
    logger = logging.getLogger('build_station_file_logger')
    logger.debug("Starting build_station_file")

  try:
    station_results_directory = config_file.get('json_settings', 'station_results_directory')

  except ConfigParser.Error, e:
    if logger:
      logger.exception(e)
  else:
    for site in fl_sites:
      if logger:
        logger.debug("Searching for site: %s in bacteria data." % (site.description.lower()))
      values = None
      if site.description.lower() in bacteria_data:
        if logger:
          logger.debug("Station: %s building file." % (site.name))

        station_data = bacteria_data[site.description.lower()]

        #Now find if we have the station file already, if not we create it.
        station_filename = os.path.join(station_results_directory, '%s.json' % (site.name))
        feature = None
        if isinstance(station_data['value'], int) or isinstance(station_data['value'], long):
          values = station_data['value']
        else:
          values = station_data['value'].split(';')
          values = [int(val.strip()) for val in values]
      elif build_missing:
        values = []
      if values is not None:
        test_data = {
          'date': station_data['sample_date'].strftime('%Y-%m-%d %H:%M:%S'),
          'station': site.name,
          'value': values
        }
        #Do we have a station file already, if so get the data.
        if os.path.isfile(station_filename):
          try:
            with open(station_filename, 'r') as station_json_file:
              feature = json.loads(station_json_file.read())
              beachadvisories = feature['properties']['test']['beachadvisories']
              #Make sure the date is not already in the list.
              if not contains(beachadvisories, lambda x: x['date'] == test_data['date']):
                if logger:
                  logger.debug("Station: %s adding date: %s" % (site.name, test_data['date']))
                beachadvisories.append(test_data)
                beachadvisories.sort(key=lambda x: x['date'], reverse=False)
          except (json.JSONDecodeError, IOError) as e:
            if logger:
              logger.exception(e)
        #No file, so let's create the station data
        else:
          feature = {
            'type': 'Feature',
            'geometry': {
              'type': 'Point',
              'coordinates': [site.object_geometry.x, site.object_geometry.y]
            },
            'properties' : {
              'locale': site.description,
              'sign': False,
              'station': site.name,
              'epaid': site.epa_id,
              'beach': site.county,
              'desc': site.description,
              'len': '',
              'test': {
                'beachadvisories': [test_data]
              }
            }
          }
        try:
          with open(station_filename, 'w') as station_json_file:
            station_json_file.write(json.dumps(feature))
        except (json.JSONDecodeError, IOError) as e:
          if logger:
            logger.exception(e)
      else:
        if logger:
          logger.debug("Site: %s not found in bacteria data." % (site.description.lower()))


  if logger:
    logger.debug("Finished build_station_file")

  return

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file", default=None,
                    help="INI Configuration file." )
  parser.add_option("-s", "--CreateCurrentSampleFile", dest="create_current", default=False,
                    help="If set, this will create the current the stations json file with the latest data.")
  parser.add_option("-i", "--CreateStationsFile", dest="create_stations", default=False,
                    help="If set, this will create or update the individual station json file with the data from latest email.")
  parser.add_option("-d", "--DateFile", dest="date_file", default=None,
                    help="If provided, full file path for a list of dates processed.")

  (options, args) = parser.parse_args()

  if options.config_file is None:
    parser.print_help()
    sys.exit(-1)

  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)
  except Exception, e:
    raise
  else:
    logger = None
    use_logging = False
    try:

      logConfFile = config_file.get('logging', 'config_file')
      boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
      sites_location_file = config_file.get('boundaries_settings', 'sample_sites')

      if logConfFile:
        logging.config.fileConfig(logConfFile)
        logger = logging.getLogger('florida_wq_bact_harvest_logger')
        logger.info("Log file opened.")
        use_logging = True
    except ConfigParser.Error, e:
      if logger:
        logger.exception(e)
    else:
      data_file_list = check_email_for_update(config_file, use_logging)
      if len(data_file_list):
        fl_sites = florida_sample_sites(True)
        fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
        data_dict = {}
        for data_file in data_file_list:
          sample_data, sample_date = parse_sheet_data(data_file, use_logging)
          data_dict[sample_date] = sample_data


        date_keys = data_dict.keys()
        #date_keys.sort()
        date_keys.sort()
        #Build the individual station json files.
        for date_key in date_keys:
          build_station_file(data_dict[date_key], config_file, fl_sites, True, use_logging)
        #Build the most current results for all the stations.
        build_current_file(data_dict[date_keys[-1]], config_file, fl_sites, True, use_logging)

        if options.date_file is not None:
          with open(options.date_file, 'w') as date_file_obj:
            if date_file_obj is not None:
              for date_key in date_keys:
                date_file_obj.write('%s,' % (date_key))
              date_file_obj.write('\n')
  return


if __name__ == "__main__":
  main()
