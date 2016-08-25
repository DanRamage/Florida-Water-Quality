import sys
from os.path import dirname, realpath
sys.path.append('../../commonfiles/python')
sys.path.append(dirname(realpath(__file__)))
from datetime import datetime
from pytz import timezone
import logging.config
import ConfigParser

from wq_prediction_plugin import wq_prediction_engine_plugin
from florida_wq_prediction_engine import fl_prediction_engine

class sarasota_prediction_plugin(wq_prediction_engine_plugin):

  def initialize_plugin(self, **kwargs):
    #self.logger.debug("inititalize_plugin Started")
    self.config_file = kwargs['ini']
    self.process_dates = kwargs.get('process_date', None)
    self.name = kwargs['name']
    #self.logger.debug("inititalize_plugin Finished")

  #def run_wq_models(self, **kwargs):
  def run(self):
    config_file = ConfigParser.RawConfigParser()
    config_file.read(self.config_file)
    logConfFile = config_file.get('logging', 'prediction_engine')

    logging.config.fileConfig(logConfFile)
    logger = logging.getLogger(self.name)
    logger.debug("run_wq_models Started. Process: %s" % (self.name))
    dates_to_process = []
    if self.process_dates is not None:
      #Can be multiple dates, so let's split on ','
      collection_date_list = self.process_dates.split(',')
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      eastern = timezone('US/Eastern')
      try:
        for collection_date in collection_date_list:
          est = eastern.localize(datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S"))
          #Convert to UTC
          begin_date = est.astimezone(timezone('UTC'))
          dates_to_process.append(begin_date)
      except Exception,e:
        self.logger.exception(e)
    else:
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      est = datetime.now(timezone('US/Eastern'))
      est = est.replace(hour=0, minute=0, second=0,microsecond=0)
      #Convert to UTC
      begin_date = est.astimezone(timezone('UTC'))
      dates_to_process.append(begin_date)

    try:
      for process_date in dates_to_process:
        fl_engine = fl_prediction_engine()
        fl_engine.run_wq_models(begin_date=process_date,
                                config_file_name=self.config_file)

        #run_wq_models(begin_date=process_date,
        #              config_file_name=self.config_file)

    except Exception, e:
      logger.exception(e)
    logger.debug("run_wq_models Finished")
    #Force the logging handlers to close. For whatever reason, most likely
    #multiprocessing, they can remain open and not send out remaining messages.
    [hand.close() for hand in logger.root.handlers]