import sys
sys.path.append('../commonfiles')
import os
import logging.config

from openpyxl import load_workbook
import poplib
import email
import optparse
import ConfigParser
import simplejson as json
from datetime import datetime

from florida_wq_data import florida_sample_sites

def check_email_for_update(config_file, use_logging):
  logger = None
  if use_logging:
    logger = logging.getLogger('check_email_for_update_logger')
    logger.debug("Starting check_email_for_update")
  try:
    email_host = config_file.get("email_settings", "host")
    email_user = config_file.get("email_settings", "user")
    email_password = config_file.get("email_settings", "password")
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
          if not(filename):
            filename = "sarasota_bacteria.xls"
          if logger:
            logger.debug("Attached filename: %s" % (filename))

          saved_file_name = os.path.join(destination_directory, filename)
          with open(saved_file_name, 'wb') as out_file:
            out_file.write(part.get_payload(decode=1))
            out_file.close()

            parse_sheet_data(saved_file_name, config_file, use_logging)

    #pop3_obj.quit()

  if logger:
    logger.debug("Finished check_email_for_update")
  return

def parse_sheet_data(xl_file_name, config_file, use_logging):
  logger = None
  if use_logging:
    logger = logging.getLogger('check_email_for_update_logger')
    logger.debug("Starting parse_sheet_data")

  try:
    advisory_results_filename = config_file.get('json_settings', 'advisory_results')
    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')


  except ConfigParser.Error, e:
    if logger:
      logger.exception(e)
  else:
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    wb = load_workbook(filename = xl_file_name)
    bacteria_data_sheet = wb['SCHD Bact Results']
    date_column = 'A'
    time_column = 'B'
    station_column = 'D'
    entero_column = 'H'
    features = []
    #Let's go site by site and find the data in the worksheet. Currently we only get the
    #data for Sarasota county.
    for site in fl_sites:
      if logger:
        logger.debug("Site: %s Desc: %s searching worsheet" % (site.name, site.description))
      for row_num in range(2,18):
        cell_name = "%s%d" % (station_column, row_num)
        cell_value = "%s%d" % (entero_column, row_num)
        cell_date = "%s%d" % (date_column, row_num)
        cell_time = "%s%d" % (time_column, row_num)
        if bacteria_data_sheet[cell_name].value.strip().lower() == site.description.lower():
          if logger:
            logger.debug("Adding feature site: %s Desc: %s" % (site.name, site.description))
          #Build the json structure the web app is going to use.
          sample_date_time = datetime.combine(bacteria_data_sheet[cell_date].value, bacteria_data_sheet[cell_time].value)
          features.append({
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
              'test': {
                'beachadvisories': {
                  'date': sample_date_time.strftime('%Y-%m-%d %H:%M:%S'),
                  'station': site.name,
                  'value': float(bacteria_data_sheet[cell_value].value)
                }
              }
            }

          })
          break
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
      logger.debug("Finished parse_sheet_data")

  return

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file", default=None,
                    help="INI Configuration file." )

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
      if logConfFile:
        logging.config.fileConfig(logConfFile)
        logger = logging.getLogger('florida_wq_bact_harvest_logger')
        logger.info("Log file opened.")
        use_logging = True
    except ConfigParser.Error, e:
      if logger:
        logger.exception(e)
    else:
      check_email_for_update(config_file, use_logging)


  return


if __name__ == "__main__":
  main()
