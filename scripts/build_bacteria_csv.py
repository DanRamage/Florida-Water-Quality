import sys
sys.path.append('../commonfiles')

import os
import logging.config
import optparse
import ConfigParser
import simplejson as json
from florida_wq_data import florida_sample_sites
from datetime import datetime

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file", default=None,
                    help="INI Configuration file." )
  parser.add_option("-o", "--CSVOutFile", dest="out_filename", default=None,
                    help="CSV file to write results." )
  parser.add_option("-d", "--DatesFile", dest="dates_file", default=None,
                    help="File to write all the unique test dates.")

  (options, args) = parser.parse_args()

  if options.config_file is None:
    parser.print_help()
    sys.exit(-1)
  logger = None
  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)
    logConfFile = config_file.get('logging', 'config_file')
    if logConfFile:
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger('florida_wq_bact_harvest_logger')
      logger.info("Log file opened.")

    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    station_results_directory = config_file.get('json_settings', 'station_results_directory')

  except Exception, e:
    raise
  else:
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    try:
      unique_dates = []
      for site in fl_sites:
        out_filename = os.path.join(options.out_filename, '%s.csv' % (site.name))
        if logger:
          logger.debug("Opening csv file: %s" % (out_filename))
        with open(out_filename, 'w') as csv_out_file:
          csv_out_file.write("Date,Value\n")
          station_filename = os.path.join(station_results_directory, '%s.json' % (site.name))
          if os.path.isfile(station_filename):
            try:
              with open(station_filename, 'r') as station_json_file:
                feature = json.loads(station_json_file.read())
                beachadvisories = feature['properties']['test']['beachadvisories']
                for advisory in beachadvisories:
                  if isinstance(advisory['value'], list):
                    csv_out_file.write("%s,%f\n" % (advisory['date'], advisory['value'][0]))
                  else:
                    csv_out_file.write("%s,%f\n" % (advisory['date'], advisory['value']))
                  sample_date = datetime.strptime(advisory['date'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT00:00:00')
                  if sample_date not in unique_dates:
                    unique_dates.append(sample_date)

            except IOError,e:
              if logger:
                logger.exception(e)

      if len(unique_dates):
        with open(options.dates_file, 'w') as dates_file_obj:
          unique_dates.sort()
          dates_file_obj.write((','.join(unique_dates)))

    except IOError, e:
      if logger:
        logger.exception(e)



  return

if __name__ == "__main__":
  main()
