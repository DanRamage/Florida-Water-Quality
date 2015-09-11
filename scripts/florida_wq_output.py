from wq_results import wq_results, _resolve
from mako.template import Template
from mako import exceptions as makoExceptions
from datetime import datetime
import simplejson as json
from smtp_utils import smtpClass
import os

class email_wq_results(wq_results):
  def __init__(self,
               mailhost,
               fromaddr,
               toaddrs,
               subject,
               user_and_password,
               results_template,
               results_outfile,
               report_url,
               use_logging):

    wq_results.__init__(self, use_logging)
    self.mailhost = mailhost
    self.mailport = None
    self.fromaddr = fromaddr
    self.toaddrs = toaddrs
    self.subject = subject
    self.user = user_and_password[0]
    self.password = user_and_password[1]
    self.result_outfile = results_outfile
    self.results_template = results_template
    self.report_url = report_url

  def emit(self, record):
    if self.logger:
      self.logger.debug("Starting emit for email output.")
    try:
      mytemplate = Template(filename=self.results_template)
      file_ext = os.path.splitext(self.result_outfile)
      file_parts = os.path.split(file_ext[0])
      #Add the prediction date into the filename
      file_name = "%s-%s%s" % (file_parts[1], record['prediction_date'].replace(':', '_').replace(' ', '-'), file_ext[1])
      out_filename = os.path.join(file_parts[0], file_name)
      with open(out_filename, 'w') as report_out_file:
        report_url = '%s/%s' % (self.report_url, file_name)
        results_report = mytemplate.render(ensemble_tests=record['ensemble_tests'],
                                                prediction_date=record['prediction_date'],
                                                execution_date=record['execution_date'],
                                                report_url=report_url)
        report_out_file.write(results_report)
    except TypeError,e:
      if self.logger:
        self.logger.exception(makoExceptions.text_error_template().render())
    except (IOError,AttributeError,Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      try:
        subject = "[WQ] Sarasota Prediction Results for %s" % (record['prediction_date'])
        #Now send the email.
        smtp = smtpClass(host=self.mailhost, user=self.user, password=self.password)
        smtp.rcpt_to(self.toaddrs)
        smtp.from_addr(self.fromaddr)
        smtp.subject(subject)
        smtp.message(results_report)
        smtp.send(content_type="html")
      except Exception,e:
        if self.logger:
          self.logger.exception(e)
    if self.logger:
      self.logger.debug("Finished emit for email output.")

class json_wq_results(wq_results):
  def __init__(self, json_outfile, use_logging):
    wq_results.__init__(self, use_logging)
    self.json_outfile = json_outfile

  def emit(self, record):
    if self.logger:
      self.logger.debug("Starting emit for json output.")

    ensemble_data = record['ensemble_tests']
    try:
      with open(self.json_outfile, 'w') as json_output_file:
        station_data = {'features' : [],
                        'type': 'FeatureCollection'}
        features = []
        for rec in ensemble_data:
          site_metadata = rec['metadata']
          test_results = rec['models']
          stats = rec['statistics']
          test_data = []
          for test in test_results.tests:
            test_data.append({
              'name': test.model_name,
              'p_level': test.predictionLevel.__str__(),
              'p_value': test.mlrResult,
              'data': test.data_used
            })
          features.append({
            'type': 'Feature',
            'geometry' : {
              'type': 'Point',
              'coordinates': [site_metadata.object_geometry.x, site_metadata.object_geometry.y]
            },
            'properties': {
              'desc': site_metadata.name,
              'ensemble': "",
              'station': site_metadata.name,
              'tests': test_data
            }
          })
        station_data['features'] = features
        json_data = {
          'status': {'http_code': 200},
          'contents': {
            'run_date': record['execution_date'],
            'testDate': record['prediction_date'],
            'stationData': station_data
          }
        }
        try:
          json_output_file.write(json.dumps(json_data, sort_keys=True))
        except Exception,e:
          if self.logger:
            self.logger.exception(e)
    except IOError,e:
      if self.logger:
        self.logger.exception(e)
    if self.logger:
      self.logger.debug("Finished emit for json output.")
    return