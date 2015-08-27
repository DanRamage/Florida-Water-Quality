from wq_results import wq_results, _resolve
from mako.template import Template
from mako import exceptions as makoExceptions
from datetime import datetime
from pytz import timezone



class email_wq_results(wq_results):
  def __init__(self,
               mailhost,
               fromaddr,
               toaddrs,
               subject,
               user_and_password,
               results_template,
               results_outfile,
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

  def emit(self, record):
    try:
      mytemplate = Template(filename=self.results_template)

      with open(self.result_outfile, 'w') as report_out_file:
        report_out_file.write(mytemplate.render(ensemble_tests=record['ensemble_tests'],
                                                prediction_date=record['prediction_date'],
                                                execution_date=record['execution_date']))
    except Exception,e:
      if self.logger:
        self.logger.exception(makoExceptions.text_error_template().render())
    except TypeError,e:
      if self.logger:
        self.logger.exception(makoExceptions.text_error_template().render())
    except IOError,e:
      if self.logger:
        self.logger.exception(e)
    except AttributeError, e:
      if self.logger:
        self.logger.exception(e)

class json_wq_results(wq_results):
  def __init__(self, json_outfile, use_logging):
    wq_results.__init__(self, use_logging)
    self.json_outfile = json_outfile

  def emit(self, record):
    return