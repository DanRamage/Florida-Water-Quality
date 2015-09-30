import sys
sys.path.append('../commonfiles')
import os
import logging.config
import csv
import glob
import optparse
import ConfigParser

from florida_wq_data import florida_sample_sites


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-d", "--DestinationDirectory", dest="dest_dir",
                    help="Destination directory to write the config files." )
  parser.add_option("-m", "--ModelCSVFileDirectory", dest="model_csv_dir",
                    help="Directory where the CSV files defining the models are located." )

  (options, args) = parser.parse_args()

  logger = None

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

  try:
    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
  except ConfigParser.Error,e:
    if logger:
      logger.exception(e)
  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    VB_FUNCTIONS_REPLACE = [
      ("POLY", "VB_POLY"),
      ("QUADROOT", "VB_QUADROOT"),
      ("SQUAREROOT", "VB_SQUAREROOT"),
      ("INVERSE", "VB_INVERSE"),
      ("SQUARE", "VB_SQUARE"),
      ("WindO_comp", "VB_WindO_comp"),
      ("WindA_comp", "VB_WindA_comp"),
      ("LOG10", "VB_LOG10")
    ]
    model_csv_list = glob.glob('%s/*.csv' % (options.model_csv_dir))
    start_line = 5
    header_row = [
      "Site",
      "Option",
      "# of Observations",
      "#  of Variables",
      "Model for Log Enterococci",
      "Highest BIC"
      "R2",
      "Adj-R2",
      "RMSE"
    ]
    for model_csv_file in model_csv_list:
      model_name = os.path.splitext(os.path.split(model_csv_file)[1])[0]
      model_config_parser = ConfigParser.ConfigParser()
      model_config_outfile_name = os.path.join(options.dest_dir, '%s.ini' % (model_name))
      try:
        with open(model_config_outfile_name, 'w') as model_config_ini_obj:
          model_count = 0
          model_config_parser.add_section("settings")
          with open(model_csv_file, "rU") as model_file_obj:
            model_file_reader = csv.DictReader(model_file_obj, delimiter=',', quotechar='"', fieldnames=header_row)
            line_num = 1
            for row in model_file_reader:
              #Actually model lines start multiple lines into the file.
              if line_num >= start_line:
                model_section = "model_%d" % (model_count+1)
                model_config_parser.add_section(model_section)
                model_config_parser.set(model_section, 'name', row['Option'])
                formula_string = row['Model for Log Enterococci']
                formula_string = formula_string.replace('[', '(')
                formula_string = formula_string.replace(']', ')')
                for vb_replacement in VB_FUNCTIONS_REPLACE:
                  formula_string = formula_string.replace(vb_replacement[0], vb_replacement[1])
                model_config_parser.set(model_section, 'formula', formula_string)
                model_count += 1

              line_num += 1
            model_config_parser.set("settings", "model_count", model_count)
            model_config_parser.write(model_config_ini_obj)
      except IOError, e:
        if logger:
          logger.exception(e)
      #    for site in fl_sites:

  logger.info("Log file closed.")

  return


if __name__ == "__main__":
  main()
