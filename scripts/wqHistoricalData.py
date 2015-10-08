import sys
import logging.config
from shapely.wkt import loads as wkt_loads
import csv

#For large polygons in the boundaries
csv.field_size_limit(sys.maxsize)

class wq_defines:
  NO_DATA = -9999.0

class prediction_levels(object):
  NO_TEST = -1
  LOW = 1
  MEDIUM = 2
  HIGH = 3
  def __init__(self, value):
    self.value = value
  def __str__(self):
    if(self.value == self.LOW):
      return "LOW"
    elif(self.value == self.MEDIUM):
      return "MEDIUM"
    elif(self.value == self.HIGH):
      return "HIGH"
    else:
      return "NO TEST"


"""
class station_data(dict):
  def __init__(self, *args, **kwargs):
      super(station_data, self).__init__(*args, **kwargs)
      self.__dict__ = self
"""
class wq_data(object):
  def __init__(self, **kwargs):
    self.logger = None
    if 'use_logger' in kwargs:
      self.logger = logging.getLogger(type(self).__name__)
    self.station_dataset = None

  """
  Function: initialize
  Purpose: Allows the object to be created once and reused/reinitialized.
  """
  def initialize(self, **kwargs):
    return False

  """
  Function: query_data
  Purpose: Function called to retrieve the data.
  """
  def query_data(self, start_date, end_date):

    return False

"""
item_geometry
COntains name of boundary and the geometry that localizes it, usually a WKT POlygon.
"""
class item_geometry(object):
  def __init__(self, name, wkt=None):
    self.name = name                          #Name for the object
    if wkt is not None:
      self.object_geometry = wkt_loads(wkt) #Shapely object

"""
station_geometry
COntains name of sample site and the geometry that localizes it, usually a WKT Point.
"""
class station_geometry(item_geometry):
  def __init__(self, name, wkt=None):
    item_geometry.__init__(self, name, wkt)
    self.contained_by = []      #THe boundaries that the station resides in.

  def add_boundary(self, name, wkt):
    self.contained_by.append(item_geometry(name, wkt))


"""
geometry_list
Class that loads simple CSV file containing the WKT and NAME of polygon boundaries.
"""
class geometry_list(list):
  def __init__(self, use_logger):
    self.logger = None
    if use_logger:
      self.logger = logging.getLogger(type(self).__name__)
  """
  Function: load
  Purpose: Loads the given CSV file, file_name, and creates a list of the boundary objects.
  Parameters:
    file_name = full path to the CSV to load. CSV file must have WKT column and NAME column.
  Return: True if successully loaded, otherwise False.
  """
  def load(self, file_name):
    header_row = ["WKT", "NAME"]
    try:
      geometry_file = open(file_name, "rU")
      if self.logger:
        self.logger.debug("Open boundary file: %s" % (file_name))
      dict_file = csv.DictReader(geometry_file, delimiter=',', quotechar='"', fieldnames=header_row)

      line_num = 0
      for row in dict_file:
        if line_num > 0:
          if self.logger:
            self.logger.debug("Building boundary polygon for: %s" % (row['NAME']))
          self.append(item_geometry(row['NAME'], row['WKT']))
        line_num += 1

      return True

    except (IOError,Exception) as e:
      if self.logger:
        self.logger.exception(e)

    return False


  def get_geometry_item(self, name):
    for geometry_item in self:
      if geometry_item.name.lower() == name.lower():
        return geometry_item
    return None

class sampling_sites(list):
  def load_sites(self, **kwargs):
    return False

  def get_site(self, site_name):
    for site in self:
      if site.name.lower() == site_name.lower():
        return site
    return None


class tide_data_file(dict):
  def __init__(self, logger=True):
    self.logger = None
    self.tide_file_name = None
    if logger:
      self.logger = logging.getLogger(type(self).__name__)

  def open(self, tide_csv_file, header_str="Station,Date,Range,Hi,Lo"):
    if self.logger:
      self.logger.debug("Opening tide file: %s" % (tide_csv_file))

    try:
      self.tide_file_name = tide_csv_file
      header_row = header_str.split(',')
      with open(tide_csv_file, "r") as tide_file:
        dict_file = csv.DictReader(tide_file, delimiter=',', quotechar='"', fieldnames=header_row)
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            try:
              self.__setitem__(row['Date'], {'station': row['Station'],
                                              'range': float(row['Range']),
                                              'hi': float(row['Hi']),
                                              'lo': float(row['Lo'])})
            except ValueError, e:
              if self.logger:
                self.logger("Error on line: %d" % (line_num))
                self.logger.exception(e)
          line_num += 1
      if self.logger:
        self.logger.debug("Processed %d lines." % (line_num))
    except (IOError,Exception) as e:
      if self.logger:
        self.logger.exception(e)


  def add_data(self, date, station, tide_range, tide_hi, tide_lo):
    self.__setitem__(date), {'station': station,
                              'range': tide_range,
                              'hi': tide_hi,
                              'lo': tide_lo}
