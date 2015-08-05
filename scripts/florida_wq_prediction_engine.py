import sys
sys.path.append('../commonfiles')

import logging.config
from datetime import datetime, timedelta
from pytz import timezone
import traceback

import optparse
import ConfigParser


from florida_wq_data import florida_wq_data, florida_sample_sites


def run_wq_models(**kwargs):

  #Load the sample site information. Has name, location and the boundaries that contain the site.
  fl_sites = florida_sample_sites(True)
  fl_sites.load_sites(file_name=kwargs['sites_location_file'], boundary_file=kwargs['boundaries_location_file'])

  return

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )

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

    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')

    run_wq_models(sites_location_file=sites_location_file, boundaries_location_file=boundaries_location_file)
  except ConfigParser.Error, e:
    traceback.print_exc(e)
    sys.exit(-1)


  return

if __name__ == "__main__":
  main()
