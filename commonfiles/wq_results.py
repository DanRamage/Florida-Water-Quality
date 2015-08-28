import logging.config
import ConfigParser


def _resolve(name):
    """Resolve a dotted name to a global object."""
    name = name.split('.')
    used = name.pop(0)
    found = __import__(used)
    for n in name:
        used = used + '.' + n
        try:
            found = getattr(found, n)
        except AttributeError:
            __import__(used)
            found = getattr(found, n)
    return found


class wq_results(object):
  def __init__(self, use_logging):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

  def emit(self, record):
    raise NotImplementedError("Must be implemented by child class")

  def flush(self):
    raise NotImplementedError("Must be implemented by child class")

  def output(self, record):
    self.handle(record)

  def handle(self, record):
    self.emit(record)


class results_exporter(object):
  def __init__(self, use_logging):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)
    self.handler_list = []

  def load_configuration(self, config_filename):
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(config_filename)
      output_list = config_file.get("output", "handlers")
    except ConfigParser.Error, e:
      raise e
    else:
      for output_type in output_list.split(','):
        try:
          klass = config_file.get('handler_%s' % (output_type), 'class')
          args = config_file.get('handler_%s' % (output_type), 'args')
          klass_obj = _resolve(klass)
          args = eval(args)
          klass_obj = klass_obj(*args)
          self.handler_list.append(klass_obj)
        except (ConfigParser.Error, Exception) as e:
          raise e

  def output(self, record):
    for handler in self.handler_list:
      handler.output(record)