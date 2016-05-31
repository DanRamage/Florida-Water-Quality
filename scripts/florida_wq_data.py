import sys
sys.path.append('../commonfiles/python')

import logging.config

from datetime import datetime, timedelta
from pytz import timezone
from shapely.geometry import Polygon
import logging.config

import netCDF4 as nc
import numpy as np
from bisect import bisect_left,bisect_right
import csv

from wqHistoricalData import wq_data
from wqXMRGProcessing import wqDB
from wqHistoricalData import station_geometry,sampling_sites, wq_defines, geometry_list
from date_time_utils import get_utc_epoch
from NOAATideData import noaaTideData
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs
from stats import calcAvgSpeedAndDir
from romsTools import closestCellFromPtInPolygon

meters_per_second_to_mph = 2.23694

def find_le(a, x):
    'Find rightmost ndx less than or equal to x'
    i = bisect_right(a, x)
    if i:
        return i-1
    raise ValueError

def find_ge(a, x):
    'Find leftmost ndx greater than or equal to x'
    i = bisect_left(a, x)
    if i != len(a):
        return i
    raise ValueError

class florida_wq_site(station_geometry):
  def __init__(self, **kwargs):
    station_geometry.__init__(self, kwargs['name'], kwargs['wkt'])
    self.epa_id = kwargs['epa_id']
    self.description = kwargs['description']
    self.county = kwargs['county']
    return
"""
florida_sample_sites
Overrides the default sampling_sites object so we can load the sites from the florida data.
"""
class florida_sample_sites(sampling_sites):
  def __init__(self, use_logger=False):
    self.logger = None
    if use_logger:
      self.logger = logging.getLogger(type(self).__name__)

  """
  Function: load_sites
  Purpose: Given the file_name in the kwargs, this will read the file and load up the sampling
    sites we are working with.
  Parameters:
    **kwargs - Must have file_name which is full path to the sampling sites csv file.
  Return:
    True if successfully loaded, otherwise False.
  """
  def load_sites(self, **kwargs):
    if 'file_name' in kwargs:
      if 'boundary_file' in kwargs:
        fl_boundaries = geometry_list(use_logger=True)
        fl_boundaries.load(kwargs['boundary_file'])

      try:
        header_row = ["WKT","EPAbeachID","SPLocation","Description","County","Boundary"]
        if self.logger:
          self.logger.debug("Reading sample sites file: %s" % (kwargs['file_name']))

        sites_file = open(kwargs['file_name'], "rU")
        dict_file = csv.DictReader(sites_file, delimiter=',', quotechar='"', fieldnames=header_row)
      except IOError, e:
        if self.logger:
          self.logger.exception(e)
      else:
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            add_site = False
            #The site could be in multiple boundaries, so let's search to see if it is.
            station = self.get_site(row['SPLocation'])
            if station is None:
              add_site = True
              """
              station_geometry.__init__(self, kwargs['name'], kwargs['wkt'])
              self.epa_id = kwargs['epa_id']
              self.description = kwargs['description']
              self.county = kwargs['county']

              """
              station = florida_wq_site(name=row['SPLocation'],
                                        wkt=row['WKT'],
                                        epa_id=row['EPAbeachID'],
                                        description=row['Description'],
                                        county=row['County'])
              self.append(station)

              boundaries =  row['Boundary'].split(',')
              for boundary in boundaries:
                if self.logger:
                  self.logger.debug("Sample site: %s Boundary: %s" % (row['SPLocation'], boundary))
                boundary_geometry = fl_boundaries.get_geometry_item(boundary)
                if add_site:
                  #Add the containing boundary
                  station.contained_by.append(boundary_geometry)
          line_num += 1
        return True
    return False

"""
florida_wq_data
Class is responsible for retrieving the data used for the sample sites models.
"""
class florida_wq_historical_data(wq_data):
  """
  Function: __init__
  Purpose: Initializes the class.
  Parameters:
    boundaries - The boundaries for the NEXRAD data the site falls within, this is required.
    xenia_database_name - The full file path to the xenia database that houses the NEXRAD and other
      data we use in the models. This is required.
  """
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)

    self.site = None
    #The main station we retrieve the values from.
    self.tide_station =  None
    #These are the settings to correct the tide for the subordinate station.
    self.tide_offset_settings =  None
    self.tide_data_obj = None

    if self.logger:
      self.logger.debug("Connecting to thredds endpoint for c10: %s" % (kwargs['c_10_tds_url']))
    #Connect to netcdf file for retrieving data from c10 buoy. To speed up retrieval, we connect
    #only once and retrieve the times.
    self.ncObj = nc.Dataset(kwargs['c_10_tds_url'])
    self.c10_times = self.ncObj.variables['time'][:]

    #Salinity before 2012-11-01 23:00:00
    self.c10_time_break = timezone('UTC').localize(datetime.strptime('2012-11-01 23:00:00', '%Y-%m-%d %H:%M:%S'))
    self.c10_water_temp_04 = self.ncObj.variables['temperature_04'][:]
    self.c10_salinity_04 = self.ncObj.variables['salinity_04'][:]

    #Salinity aftter 2012-11-01 23:00:00
    self.c10_water_temp_01 = self.ncObj.variables['temperature_01'][:]
    self.c10_salinity_01 = self.ncObj.variables['salinity_01'][:]

    self.model_bbox = kwargs['model_bbox']
    self.model_within_polygon = Polygon(kwargs['model_within_polygon'])


    if self.logger:
      self.logger.debug("Connecting to thredds endpoint for hycom data: %s" % (kwargs['hycom_model_tds_url']))
    self.hycom_model = nc.Dataset(kwargs['hycom_model_tds_url'])

    self.hycom_model_time = self.hycom_model.variables['MT'][:]
    model_bbox = [float(self.model_bbox[0]),float(self.model_bbox[2]),
                          float(self.model_bbox[1]),float(self.model_bbox[3])]

    #Determine the bounding box indexes.
    lons = self.hycom_model.variables['Longitude'][:]
    lats = self.hycom_model.variables['Latitude'][:]
    # latitude lower and upper index
    self.hycom_latli = np.argmin( np.abs( lats - model_bbox[2] ) )
    self.hycom_latui = np.argmin( np.abs( lats - model_bbox[3] ) )
    # longitude lower and upper index
    self.hycom_lonli = np.argmin( np.abs( lons - model_bbox[0] ) )
    self.hycom_lonui = np.argmin( np.abs( lons - model_bbox[1] ) )

    self.hycom_lon_array = self.hycom_model.variables['Longitude'][self.hycom_lonli:self.hycom_lonui]
    self.hycom_lat_array = self.hycom_model.variables['Latitude'][self.hycom_latli:self.hycom_latui]

    if self.logger:
      self.logger.debug("Connection to xenia db: %s" % (kwargs['xenia_database_name']))
    self.xenia_db = wqDB(kwargs['xenia_database_name'], type(self).__name__)

  def __del__(self):
    if self.logger:
      self.logger.debug("Closing connection to xenia db")
    self.xenia_db.DB.close()

    if self.logger:
      self.logger.debug("Closing connection to thredds endpoint.")
    self.ncObj.close()

    if self.logger:
      self.logger.debug("Closing connection to hycom endpoint.")
    self.hycom_model.close()

  def reset(self, **kwargs):
    self.site = kwargs['site']
    #The main station we retrieve the values from.
    self.tide_station = kwargs['tide_station']
    #These are the settings to correct the tide for the subordinate station.
    self.tide_offset_settings = kwargs['tide_offset_params']

    self.tide_data_obj = None
    if 'tide_data_obj' in kwargs and kwargs['tide_data_obj'] is not None:
      self.tide_data_obj = kwargs['tide_data_obj']

  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """
  def query_data(self, start_date, end_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Site: %s start query data for datetime: %s" % (self.site.name, start_date))

    self.initialize_return_data(wq_tests_data)

    self.get_tide_data(start_date, wq_tests_data)
    self.get_nws_data(start_date, wq_tests_data)
    self.get_c10_data(start_date, wq_tests_data)
    self.get_nexrad_data(start_date, wq_tests_data)
    self.get_hycom_model_data(start_date, wq_tests_data)

    if self.logger:
      self.logger.debug("Site: %s Finished query data for datetime: %s" % (self.site.name, start_date))

  """
  Function: initialize_return_data
  Purpose: INitialize our ordered dict with the data variables and assign a NO_DATA
    initial value.
  Parameters:
    wq_tests_data - An OrderedDict that is initialized.
  Return:
    None
  """
  def initialize_return_data(self, wq_tests_data):
    if self.logger:
      self.logger.debug("Creating and initializing data dict.")
    #Build variables for the base tide station.
    var_name = 'tide_range_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_stage_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA

    #Build variables for the subordinate tide station.
    var_name = 'tide_range_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA

    wq_tests_data['nws_ksrq_avg_wspd'] = wq_defines.NO_DATA
    wq_tests_data['nws_ksrq_avg_wdir'] = wq_defines.NO_DATA

    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 192, 24):
          clean_var_boundary_name = boundary.name.lower().replace(' ', '_')
          var_name = '%s_nexrad_summary_%d' % (clean_var_boundary_name, prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_dry_days_count' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_rainfall_intesity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA


    for prev_hours in range(24, 192, 24):
      wq_tests_data['c10_avg_salinity_%d' % (prev_hours)] = wq_defines.NO_DATA
      wq_tests_data['c10_min_salinity_%d' % (prev_hours)] = wq_defines.NO_DATA
      wq_tests_data['c10_max_salinity_%d' % (prev_hours)] = wq_defines.NO_DATA

    wq_tests_data['c10_avg_water_temp_24'] = wq_defines.NO_DATA
    wq_tests_data['c10_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['c10_max_water_temp'] = wq_defines.NO_DATA

    for hour in range(24,192,24):
      wq_tests_data['hycom_avg_salinity_%d' % (hour)] = wq_defines.NO_DATA
      wq_tests_data['hycom_min_salinity_%d' % (hour)] = wq_defines.NO_DATA
      wq_tests_data['hycom_max_salinity_%d' % (hour)] = wq_defines.NO_DATA

    #wq_tests_data['hycom_avg_salinity_24'] = wq_defines.NO_DATA
    #wq_tests_data['hycom_min_salinity'] = wq_defines.NO_DATA
    #wq_tests_data['hycom_max_salinity'] = wq_defines.NO_DATA

    wq_tests_data['hycom_avg_water_temp_24'] = wq_defines.NO_DATA
    wq_tests_data['hycom_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['hycom_max_water_temp'] = wq_defines.NO_DATA

    """
    wq_tests_data['ncsu_avg_salinity_24'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_min_salinity'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_max_salinity'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_avg_water_temp_24'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_max_water_temp'] = wq_defines.NO_DATA
    """
    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return

  def get_hycom_model_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving hycom model data: %s" % (start_date))

    #Hycom time is referenced in days since 1900-12-31.
    beginning_time = timezone('UTC').localize(datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S'))
    begin_date = start_date - timedelta(hours=192)
    end_date = start_date
    #Verify that the date we are interested should be in the hycom model data.
    if begin_date >= (beginning_time + timedelta(days=self.hycom_model_time[0])):
      start_time_delta = begin_date - beginning_time
      end_time_delta = end_date - beginning_time

      #The time dimension in the model is days offset since the beginning_time above.
      #offset_hours = (time_delta.days * 24) + (time_delta.seconds / (60 * 60 * 24))
      offset_start = (start_time_delta.days) + (start_time_delta.seconds / (60.0 * 60.0 * 24.0))
      offset_end = (end_time_delta.days) + (end_time_delta.seconds / (60.0 * 60.0 * 24.0))
      closest_start_ndx = bisect_left(self.hycom_model_time, offset_start)
      closest_end_ndx = bisect_left(self.hycom_model_time, offset_end)
      if closest_start_ndx != -1 and closest_end_ndx != -1:
        """
        with open("/Users/danramage/tmp/salinity_model.csv", "w") as out_file:
          out_file.write("Latitude,Longitude,Lat Ndx,Lon Ndx\n")
          for lon_ndx in range(0,len(lon_array)):
            for lat_ndx in range(0,len(lat_array)):
              out_file.write("%f,%f,%d,%d\n" % (lat_array[lat_ndx], lon_array[lon_ndx], lat_ndx, lon_ndx))
        """
        try:
          if self.logger:
            self.logger.debug("Retrieving hycom salinity data.")
          salinity_data = self.hycom_model.variables['salinity'][closest_start_ndx:closest_end_ndx+1,0,self.hycom_latli:self.hycom_latui,self.hycom_lonli:self.hycom_lonui]
          pt = closestCellFromPtInPolygon(self.site.object_geometry,
                                          self.hycom_lon_array, self.hycom_lat_array,
                                          salinity_data[0],
                                          self.hycom_model.variables['salinity']._FillValue,
                                          self.model_within_polygon)
          #closest_end_ndx and closest_start_ndx are calculated from the full data set.
          #When we slice the piece we're interested in above, that dataset becomes 0
          #referenced, so when accessing salinity_data, our indexes need to reflect that.
          #salinity_192 = salinity_data[0:closest_end_ndx-closest_start_ndx+1,pt.y,pt.x]
          #times_192 = self.hycom_model_time[closest_start_ndx:closest_end_ndx+1]
          """
          with open("/Users/danramage/tmp/salinity_192_model.csv", "w") as out_file:
            out_file.write("Date,Salinity\n")
            for ndx in range(0, len(salinity_192)):
              out_file.write("%s,%f\n" %\
                             ((beginning_time + timedelta(days=times_192[ndx])).strftime("%Y-%m-%dT%H:%M:%S"),
                             salinity_192[ndx]))
          """
          adjusted_end_ndx = None
          for hour in range(24, 192, 24):
            #Calculate the date we will start at referenced to the beginning_time of the data.
            begin_delta = (start_date - timedelta(hours=hour)) - beginning_time
            #Get the days count.
            begin_days_cnt = (begin_delta.days) + (begin_delta.seconds / (60.0 * 60.0 * 24.0))
            #Find the index in the data our days count is closest to.
            begin_ndx = bisect_left(self.hycom_model_time, begin_days_cnt)
            #Offset indexes from the larger dataset to our subset.
            adjusted_begin_ndx = begin_ndx-closest_start_ndx
            if adjusted_end_ndx is None:
              adjusted_end_ndx = closest_end_ndx-closest_start_ndx
            #Validate we're getting the same dates.
            #if self.hycom_model_time[begin_ndx] != times_192[adjusted_begin_ndx]:
            #  if self.logger:
            #    self.logger.error("Times do not match")
            #Get the last 24 hour average salinity data
            avg_salinity_pts = salinity_data[adjusted_begin_ndx:adjusted_end_ndx,pt.y,pt.x]

            wq_tests_data['hycom_avg_salinity_%d' % (hour)] = float(np.average(avg_salinity_pts))
            wq_tests_data['hycom_min_salinity_%d' % (hour)] = float(avg_salinity_pts.min())
            wq_tests_data['hycom_max_salinity_%d' % (hour)] = float(avg_salinity_pts.max())

            adjusted_end_ndx = adjusted_begin_ndx

          #Calc indexes for the last 24 hour average.
          """
          last_24_delta = (start_date - timedelta(hours=24)) - beginning_time
          last_24_start = (last_24_delta.days) + (last_24_delta.seconds / (60.0 * 60.0 * 24.0))
          last_24_ndx = bisect_left(self.hycom_model_time, last_24_start)
          #Offset indexes from the larger dataset to our subset.
          start_ndx_24 = last_24_ndx-closest_start_ndx
          end_ndx_24 = closest_end_ndx-closest_start_ndx

          #Get the last 24 hour average salinity data
          avg_salinity_pts = salinity_data[start_ndx_24:end_ndx_24,pt.y,pt.x]
          """
          #times_24 = self.hycom_model_time[last_24_ndx:closest_end_ndx]
          """
          with open("/Users/danramage/tmp/salinity_24_model.csv", "w") as out_file:
            out_file.write("Date,Salinity\n")
            for ndx in range(0, len(avg_salinity_pts)):
              out_file.write("%s,%f\n" %\
                             ((beginning_time + timedelta(days=times_24[ndx])).strftime("%Y-%m-%dT%H:%M:%S"),
                             avg_salinity_pts[ndx]))
          """
          #wq_tests_data['hycom_avg_salinity_24'] = np.average(avg_salinity_pts)
          #wq_tests_data['hycom_min_salinity'] = avg_salinity_pts.min()
          #wq_tests_data['hycom_max_salinity'] = avg_salinity_pts.max()

          """
          #Now get the rate of change from 24-192 hours back
          ndx_list = []
          for time_ndx in range(24, 192, 24):
            date_ndx = start_date - timedelta(hours=time_ndx)
            time_delta = date_ndx - beginning_time
            #The time dimension in the model is hours offset since the beginning_time above.
            #offset_hours = (time_delta.days * 24) + (time_delta.seconds / (60 * 60 * 24))
            offset_ndx = (time_delta.days) + (time_delta.seconds / (60.0 * 60.0 * 24.0))
            ndx_list.append(bisect_left(self.hycom_model_time, offset_ndx))
          """
          if self.logger:
            self.logger.debug("Retrieving hycom water temp data.")

          last_24_delta = (start_date - timedelta(hours=24)) - beginning_time
          last_24_start = (last_24_delta.days) + (last_24_delta.seconds / (60.0 * 60.0 * 24.0))
          last_24_ndx = bisect_left(self.hycom_model_time, last_24_start)

          water_temp_data = self.hycom_model.variables['temperature'][last_24_ndx:closest_end_ndx,0,self.hycom_latli:self.hycom_latui,self.hycom_lonli:self.hycom_lonui]
          avg_water_temp_pts = water_temp_data[0:last_24_ndx-closest_start_ndx,pt.y,pt.x]
          #Get the water temperature data
          wq_tests_data['hycom_avg_water_temp_24'] = float(np.average(avg_water_temp_pts))
          wq_tests_data['hycom_min_water_temp'] = float(avg_water_temp_pts.min())
          wq_tests_data['hycom_max_water_temp'] = float(avg_water_temp_pts.max())
          if self.logger:
            cell_lat = self.hycom_lat_array[pt.y]
            cell_lon = self.hycom_lon_array[pt.x]
            begin_dt = beginning_time + timedelta(days=(self.hycom_model_time[closest_start_ndx]))
            end_dt = beginning_time + timedelta(days=(self.hycom_model_time[closest_end_ndx]))
            self.logger.debug("Site: %s Dates: %s to %s closest cell@ Lat: %f(%d) Lon: %f(%d) Salinity 24 hrAvg: %f Water Temp Avg: %f"\
                              % (self.site.name,\
                                 begin_dt.strftime('%Y-%m-%dT%H:%M:%S'), end_dt.strftime('%Y-%m-%dT%H:%M:%S'),\
                                 cell_lat, pt.x, cell_lon, pt.y,\
                                 wq_tests_data['hycom_avg_salinity_24'],wq_tests_data['hycom_avg_water_temp_24']))

        except Exception, e:
          if self.logger:
            self.logger.exception(e)

      else:
        if self.logger:
          self.logger.error("Cannot find start: %s or end: %s date range." % (offset_start, offset_end))
    else:
      if self.logger:
        self.logger.error("Date: %s out of range of data source." % (start_date))

    if self.logger:
      self.logger.debug("Finished retrieving hycom model data: %s" % (start_date))
    return


  def get_tide_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    use_web_service = True
    if self.tide_data_obj is not None:
      use_web_service = False
      date_key = start_date.strftime('%Y-%m-%dT%H:%M:%S')
      if date_key in self.tide_data_obj:
        tide_rec = self.tide_data_obj[date_key]
        wq_tests_data['tide_range_%s' % (self.tide_station)] = tide_rec['range']
        wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_rec['hi']
        wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_rec['lo']

        try:
          #Get previous 24 hours.
          tide_start_time = (start_date - timedelta(hours=24))
          tide_end_time = start_date

          tide = noaaTideData(use_raw=True, logger=self.logger)

          tide_stage = tide.get_tide_stage(begin_date = tide_start_time,
                             end_date = tide_end_time,
                             station=self.tide_station,
                             datum='MLLW',
                             units='feet',
                             time_zone='GMT')
          wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_stage

        except Exception,e:
          if self.logger:
            self.logger.exception(e)

      #THe web service is unreliable, so if we were using the history csv file, it may still be missing
      #a record, if so, let's try to look it up on the web.
      else:
        use_web_service = True
    if self.tide_data_obj is None or use_web_service:
      #Get previous 24 hours.
      tide_start_time = (start_date - timedelta(hours=24))
      tide_end_time = start_date

      tide = noaaTideData(use_raw=True, logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD

      try:
        tide_data = tide.calcTideRange(beginDate = tide_start_time,
                           endDate = tide_end_time,
                           station=self.tide_station,
                           datum='MLLW',
                           units='feet',
                           timezone='GMT',
                           smoothData=False)

      except Exception,e:
        if self.logger:
          self.logger.exception(e)
      else:
        if tide_data and tide_data['HH'] is not None and tide_data['LL'] is not None:
          try:
            range = tide_data['HH']['value'] - tide_data['LL']['value']
          except TypeError, e:
            if self.logger:
              self.logger.exception(e)
          else:
            #Save tide station values.
            wq_tests_data['tide_range_%s' % (self.tide_station)] = range
            wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_data['HH']['value']
            wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_data['LL']['value']
            wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_data['tide_stage']
        else:
          if self.logger:
            self.logger.error("Tide data for station: %s date: %s not available or only partial." % (self.tide_station, start_date))

    #Save subordinate station values
    if wq_tests_data['tide_hi_%s'%(self.tide_station)] != wq_defines.NO_DATA:
      offset_hi = wq_tests_data['tide_hi_%s'%(self.tide_station)] * self.tide_offset_settings['hi_tide_height_offset']
      offset_lo = wq_tests_data['tide_lo_%s'%(self.tide_station)] * self.tide_offset_settings['lo_tide_height_offset']

      tide_station = self.tide_offset_settings['tide_station']
      wq_tests_data['tide_range_%s' % (tide_station)] = offset_hi - offset_lo
      wq_tests_data['tide_hi_%s' % (tide_station)] = offset_hi
      wq_tests_data['tide_lo_%s' % (tide_station)] = offset_lo

    if self.logger:
      self.logger.debug("Finished retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    return

  def get_nws_data(self, start_date, wq_tests_data):
    platform_handle = 'nws.ksrq.met'

    if self.logger:
      self.logger.debug("Start retrieving nws platform: %s data datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

    end_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    stop_data = start_date - timedelta(hours=24)
    begin_date_str = stop_data.strftime('%Y-%m-%dT%H:%M:%S')
    avgWindComponents = self.xenia_db.calcAvgWindSpeedAndDir(platform_handle,
                                         'wind_speed', 'mph', 'wind_from_direction', 'degrees_true',
                                         begin_date_str, end_date_str)
    if avgWindComponents[1][0] != None and avgWindComponents[1][1] != None:
      wq_tests_data['nws_ksrq_avg_wspd'] = avgWindComponents[1][0]
      wq_tests_data['nws_ksrq_avg_wdir'] = avgWindComponents[1][1]

    if self.logger:
      self.logger.debug("Finished retrieving nws platform: %s data datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

    return

  def get_nexrad_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    #Collect the radar data for the boundaries.
    for boundary in self.site.contained_by:
      clean_var_bndry_name = boundary.name.lower().replace(' ', '_')

      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      if self.logger:
        self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 192, 24):
        var_name = '%s_nexrad_summary_%d' % (clean_var_bndry_name, prev_hours)
        radar_val = self.xenia_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                    start_date,
                                                                    prev_hours,
                                                                    'precipitation_radar_weighted_average',
                                                                    'mm')
        if radar_val != None:
          wq_tests_data[var_name] = radar_val
        else:
          if self.logger:
            self.logger.error("No data available for boundary: %s Date: %s. Error: %s" %(var_name, start_date, self.xenia_db.getErrorInfo()))

      #calculate the X day delay totals
      if wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_1_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_2_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_3_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)]

      prev_dry_days = self.xenia_db.getPrecedingRadarDryDaysCount(platform_handle,
                                             start_date,
                                             'precipitation_radar_weighted_average',
                                             'mm')
      if prev_dry_days is not None:
        var_name = '%s_nexrad_dry_days_count' % (clean_var_bndry_name)
        wq_tests_data[var_name] = prev_dry_days

      rainfall_intensity = self.xenia_db.calcRadarRainfallIntensity(platform_handle,
                                                               start_date,
                                                               60,
                                                              'precipitation_radar_weighted_average',
                                                              'mm')
      if rainfall_intensity is not None:
        var_name = '%s_nexrad_rainfall_intesity' % (clean_var_bndry_name)
        wq_tests_data[var_name] = rainfall_intensity


      if self.logger:
        self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))

    if self.logger:
      self.logger.debug("Finished retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))


  def get_c10_data(self, start_date, wq_tests_data):
    #Find the starting time index to work from.
    if self.logger:
      self.logger.debug("Start thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    start_epoch_time = int(get_utc_epoch(start_date) + 0.5)
    end_epoch_time = None
    for prev_hour in range(24,192,24):
      start_epoch_time = float(get_utc_epoch(start_date - timedelta(hours=prev_hour)))
      if end_epoch_time is None:
        end_epoch_time = float(get_utc_epoch(start_date))

      closest_start_ndx = bisect_left(self.c10_times, start_epoch_time)
      #For whatever reason, when the value is in the array, bisect keeps returning the index after it.
      #Convert to int with bankers rounding to overcome floating point issues.
      #I suspect the issue is floating point precision, but not 100% sure.
      if np.int64(start_epoch_time + 0.5) == np.int64(self.c10_times[closest_start_ndx-1] + np.float64(0.5)):
        closest_start_ndx -= 1
      closest_end_ndx = bisect_left(self.c10_times, end_epoch_time)
      #Check to make sure end index is not past our desired time.
      if np.int64(end_epoch_time + 0.5) == np.int64(self.c10_times[closest_end_ndx-1] + np.float64(0.5)):
        closest_end_ndx -= 1

      if self.logger:
        start_time = timezone('US/Eastern').localize(datetime.fromtimestamp(np.int64(self.c10_times[closest_start_ndx] + 0.5))).astimezone(timezone('UTC'))
        end_time = timezone('US/Eastern').localize(datetime.fromtimestamp(np.int64(self.c10_times[closest_end_ndx] + 0.5))).astimezone(timezone('UTC'))
        self.logger.debug("Thredds C10 Data Start Time: %s Ndx: %d End Time: %s Ndx: %d"\
          % (start_time.strftime('%Y-%m-%dT%H:%M:%S %Z'), closest_start_ndx, end_time.strftime('%Y-%m-%dT%H:%M:%S %Z'), closest_end_ndx))

      if ((np.int64(self.c10_times[closest_start_ndx] + 0.5) >= np.int64(start_epoch_time + 0.5)) and (np.int64(self.c10_times[closest_start_ndx] + 0.5) < np.int64(end_epoch_time + 0.5))) and\
         ((np.int64(self.c10_times[closest_end_ndx] + 0.5) > np.int64(start_epoch_time + 0.5)) and (np.int64(self.c10_times[closest_end_ndx] + 0.5) <= np.int64(end_epoch_time + 0.5))):

        if start_date <= self.c10_time_break:
          c10_salinity = self.c10_salinity_04[closest_start_ndx:closest_end_ndx]
        else:
          c10_salinity = self.c10_salinity_01[closest_start_ndx:closest_end_ndx]

        #Only calc average on real values, ignore NaNs.
        c10_no_nan_salinity = c10_salinity[~np.isnan(c10_salinity)]
        if len(c10_no_nan_salinity):
          wq_tests_data['c10_avg_salinity_%d' % (prev_hour)] = np.average(c10_no_nan_salinity)
          wq_tests_data['c10_min_salinity_%d' % (prev_hour)] = c10_no_nan_salinity.min()
          wq_tests_data['c10_max_salinity_%d' % (prev_hour)] = c10_no_nan_salinity.max()
        #Only get the 24 hour average of water temp
        if prev_hour == 24:
          #Only calc average on real values, ignore NaNs.
          if start_date <= self.c10_time_break:
            c10_water_temp = self.c10_water_temp_04[closest_start_ndx:closest_end_ndx]
          else:
            c10_water_temp = self.c10_water_temp_01[closest_start_ndx:closest_end_ndx]

          c10_no_nan_water_temp = c10_water_temp[~np.isnan(c10_water_temp)]
          if len(c10_no_nan_water_temp):
            wq_tests_data['c10_avg_water_temp_24'] = np.average(c10_no_nan_water_temp)
            wq_tests_data['c10_min_water_temp'] = c10_no_nan_water_temp.min()
            wq_tests_data['c10_max_water_temp'] = c10_no_nan_water_temp.max()

      end_epoch_time = start_epoch_time

    if self.logger:
      self.logger.debug("Thredds C10 Start: %s Avg Salinity: %s (%s,%s) Avg Water Temp: %s (%s,%s)"\
                        % (start_date.strftime('%Y-%m-%d %H:%M:%S'),
                           str(wq_tests_data['c10_avg_salinity_24']), str(wq_tests_data['c10_min_salinity_24']), str(wq_tests_data['c10_max_salinity_24']),
                           str(wq_tests_data['c10_avg_water_temp_24']), str(wq_tests_data['c10_min_water_temp']), str(wq_tests_data['c10_max_water_temp'])))
    if self.logger:
      self.logger.debug("Finished thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    return

class florida_wq_model_data(florida_wq_historical_data):
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)

    self.site = None
    #The main station we retrieve the values from.
    self.tide_station =  None
    #These are the settings to correct the tide for the subordinate station.
    self.tide_offset_settings =  None
    self.tide_data_obj = None
    self.query_tide_data = True

    if self.logger:
      self.logger.debug("Connecting to thredds endpoint for c10: %s" % (kwargs['c_10_tds_url']))
    #Connect to netcdf file for retrieving data from c10 buoy. To speed up retrieval, we connect
    try:
      #only once and retrieve the times.
      self.ncObj = nc.Dataset(kwargs['c_10_tds_url'])
      self.c10_times = self.ncObj.variables[kwargs['c_10_time_var']][:]

      self.c10_water_temp = self.ncObj.variables[kwargs['c_10_water_temp_var']][:]
      self.c10_water_temp_fill_value = self.ncObj.variables[kwargs['c_10_water_temp_var']]._FillValue
      self.c10_salinity = self.ncObj.variables[kwargs['c_10_salinity_var']][:]
      self.c10_salinity_fill_value = self.ncObj.variables[kwargs['c_10_salinity_var']]._FillValue

      self.model_bbox = kwargs['model_bbox']
      self.model_within_polygon = Polygon(kwargs['model_within_polygon'])

      if self.logger:
        self.logger.debug("Connecting to thredds endpoint for hycom data: %s" % (kwargs['hycom_model_tds_url']))
      self.hycom_model = nc.Dataset(kwargs['hycom_model_tds_url'])

      self.hycom_model_time = self.hycom_model.variables['MT'][:]
      model_bbox = [float(self.model_bbox[0]),float(self.model_bbox[2]),
                            float(self.model_bbox[1]),float(self.model_bbox[3])]

      #Determine the bounding box indexes.
      lons = self.hycom_model.variables['Longitude'][:]
      lats = self.hycom_model.variables['Latitude'][:]
      # latitude lower and upper index
      self.hycom_latli = np.argmin( np.abs( lats - model_bbox[2] ) )
      self.hycom_latui = np.argmin( np.abs( lats - model_bbox[3] ) )
      # longitude lower and upper index
      self.hycom_lonli = np.argmin( np.abs( lons - model_bbox[0] ) )
      self.hycom_lonui = np.argmin( np.abs( lons - model_bbox[1] ) )

      self.hycom_lon_array = self.hycom_model.variables['Longitude'][self.hycom_lonli:self.hycom_lonui]
      self.hycom_lat_array = self.hycom_model.variables['Latitude'][self.hycom_latli:self.hycom_latui]

      if self.logger:
        self.logger.debug("Connection to xenia db: %s" % (kwargs['xenia_database_name']))
      self.xenia_db = wqDB(kwargs['xenia_database_name'], type(self).__name__)

      #Connect to the xenia database we use for observations aggregation.
      self.xenia_obs_db = xeniaAlchemy()
      if self.xenia_obs_db.connectDB(kwargs['xenia_obs_db_type'], kwargs['xenia_obs_db_user'], kwargs['xenia_obs_db_password'], kwargs['xenia_obs_db_host'], kwargs['xenia_obs_db_name'], False):
        if self.logger:
          self.logger.info("Succesfully connect to DB: %s at %s" %(kwargs['xenia_obs_db_name'],kwargs['xenia_obs_db_host']))
      else:
        self.logger.error("Unable to connect to DB: %s at %s." %(kwargs['xenia_obs_db_name'],kwargs['xenia_obs_db_host']))


    except Exception,e:
      if self.logger:
        self.logger.exception(e)
      raise

  def __del__(self):
    florida_wq_historical_data.__del__(self)
    if self.logger:
      self.logger.debug("Disconnecting xenia obs database.")
    self.xenia_obs_db.disconnect()


  """
  Function: initialize_return_data
  Purpose: INitialize our ordered dict with the data variables and assign a NO_DATA
    initial value.
  Parameters:
    wq_tests_data - An OrderedDict that is initialized.
  Return:
    None
  """
  def initialize_return_data(self, wq_tests_data, initialize_site_specific_data_only):
    if self.logger:
      self.logger.debug("Creating and initializing data dict.")

    if not initialize_site_specific_data_only:

      wq_tests_data['nws_ksrq_avg_wspd'] = wq_defines.NO_DATA
      wq_tests_data['nws_ksrq_avg_wdir'] = wq_defines.NO_DATA

      for prev_hours in range(24, 192, 24):
        wq_tests_data['c10_avg_salinity_%d' % (prev_hours)] = wq_defines.NO_DATA
        wq_tests_data['c10_min_salinity_%d' % (prev_hours)] = wq_defines.NO_DATA
        wq_tests_data['c10_max_salinity_%d' % (prev_hours)] = wq_defines.NO_DATA

      wq_tests_data['c10_avg_water_temp_24'] = wq_defines.NO_DATA
      wq_tests_data['c10_min_water_temp'] = wq_defines.NO_DATA
      wq_tests_data['c10_max_water_temp'] = wq_defines.NO_DATA

    #Build variables for the base tide station.
    var_name = 'tide_range_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    #Build variables for the subordinate tide station.
    var_name = 'tide_range_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA

    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 192, 24):
          clean_var_boundary_name = boundary.name.lower().replace(' ', '_')
          var_name = '%s_nexrad_summary_%d' % (clean_var_boundary_name, prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_dry_days_count' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_rainfall_intesity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

    for hour in range(24,192,24):
      wq_tests_data['hycom_avg_salinity_%d' % (hour)] = wq_defines.NO_DATA
      wq_tests_data['hycom_min_salinity_%d' % (hour)] = wq_defines.NO_DATA
      wq_tests_data['hycom_max_salinity_%d' % (hour)] = wq_defines.NO_DATA

    wq_tests_data['hycom_avg_water_temp_24'] = wq_defines.NO_DATA
    wq_tests_data['hycom_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['hycom_max_water_temp'] = wq_defines.NO_DATA

    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return

  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """
  def query_data(self, start_date, end_date, wq_tests_data, reset_site_specific_data_only):
    if self.logger:
      self.logger.debug("Site: %s start query data for datetime: %s" % (self.site.name, start_date))

    self.initialize_return_data(wq_tests_data, reset_site_specific_data_only)

    #If we are resetting only the site specific data, no need to re-query these.
    if not reset_site_specific_data_only:
      self.get_nws_data(start_date, wq_tests_data)
      self.get_c10_data(start_date, wq_tests_data)

    self.get_tide_data(start_date, wq_tests_data)
    self.get_nexrad_data(start_date, wq_tests_data)
    self.get_hycom_model_data(start_date, wq_tests_data)

    if self.logger:
      self.logger.debug("Site: %s Finished query data for datetime: %s" % (self.site.name, start_date))


  def get_c10_data(self, start_date, wq_tests_data):
    #Find the starting time index to work from.
    if self.logger:
      self.logger.debug("Start thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    start_epoch_time = int(get_utc_epoch(start_date) + 0.5)
    end_epoch_time = None
    for prev_hour in range(24,192,24):
      start_epoch_time = float(get_utc_epoch(start_date - timedelta(hours=prev_hour)))
      if end_epoch_time is None:
        end_epoch_time = float(get_utc_epoch(start_date))
      try:
        closest_start_ndx = find_ge(self.c10_times, start_epoch_time)
        closest_end_ndx = find_le(self.c10_times, end_epoch_time)
      except ValueError,e:
        if self.logger:
          self.logger.error("Error finding start or end times. Number C10 times: %d latest epoch: %f" % (self.c10_times.shape[0], self.c10_times[-1]))
          self.logger.exception(e)
      else:
        if closest_start_ndx < len(self.c10_times) and closest_end_ndx < len(self.c10_times):
          #For whatever reason, when the value is in the array, bisect keeps returning the index after it.
          #Convert to int with bankers rounding to overcome floating point issues.
          #I suspect the issue is floating point precision, but not 100% sure.
          if np.int64(start_epoch_time + 0.5) == np.int64(self.c10_times[closest_start_ndx-1] + np.float64(0.5)):
            closest_start_ndx -= 1
          #Check to make sure end index is not past our desired time.
          if np.int64(end_epoch_time + 0.5) == np.int64(self.c10_times[closest_end_ndx-1] + np.float64(0.5)):
            closest_end_ndx -= 1

          if self.logger:
            start_time = timezone('US/Eastern').localize(datetime.fromtimestamp(np.int64(self.c10_times[closest_start_ndx] + 0.5))).astimezone(timezone('UTC'))
            end_time = timezone('US/Eastern').localize(datetime.fromtimestamp(np.int64(self.c10_times[closest_end_ndx] + 0.5))).astimezone(timezone('UTC'))
            self.logger.debug("Thredds C10 Data Start Time: %s Ndx: %d End Time: %s Ndx: %d"\
              % (start_time.strftime('%Y-%m-%dT%H:%M:%S %Z'), closest_start_ndx, end_time.strftime('%Y-%m-%dT%H:%M:%S %Z'), closest_end_ndx))

          if ((np.int64(self.c10_times[closest_start_ndx] + 0.5) >= np.int64(start_epoch_time + 0.5)) and (np.int64(self.c10_times[closest_start_ndx] + 0.5) < np.int64(end_epoch_time + 0.5))) and\
             ((np.int64(self.c10_times[closest_end_ndx] + 0.5) > np.int64(start_epoch_time + 0.5)) and (np.int64(self.c10_times[closest_end_ndx] + 0.5) <= np.int64(end_epoch_time + 0.5))):

            c10_salinity = self.c10_salinity[closest_start_ndx:closest_end_ndx]

            #Only calc average on real values, ignore NaNs.
            #c10_no_nan_salinity = c10_salinity[~c10_salinity.mask]
            c10_no_nan_salinity = c10_salinity[(c10_salinity > int(self.c10_salinity_fill_value))]
            c10_no_nan_salinity = c10_no_nan_salinity[~np.isnan(c10_no_nan_salinity)]
            if len(c10_no_nan_salinity):
              wq_tests_data['c10_avg_salinity_%d' % (prev_hour)] = float(np.average(c10_no_nan_salinity))
              wq_tests_data['c10_min_salinity_%d' % (prev_hour)] = float(c10_no_nan_salinity.min())
              wq_tests_data['c10_max_salinity_%d' % (prev_hour)] = float(c10_no_nan_salinity.max())
            #Only get the 24 hour average of water temp
            if prev_hour == 24:
              #Only calc average on real values, mask out the fill values and  NaNs.
              c10_water_temp = self.c10_water_temp[closest_start_ndx:closest_end_ndx]

              #c10_no_nan_water_temp = c10_water_temp[~c10_water_temp.mask]

              c10_no_nan_water_temp = c10_water_temp[(c10_water_temp > int(self.c10_water_temp_fill_value))]
              c10_no_nan_water_temp = c10_no_nan_water_temp[~np.isnan(c10_no_nan_water_temp)]
              if len(c10_no_nan_water_temp):
                wq_tests_data['c10_avg_water_temp_24'] = float(np.average(c10_no_nan_water_temp))
                wq_tests_data['c10_min_water_temp'] = float(c10_no_nan_water_temp.min())
                wq_tests_data['c10_max_water_temp'] = float(c10_no_nan_water_temp.max())

          end_epoch_time = start_epoch_time
        else:
          if self.logger:
            if closest_start_ndx == len(self.c10_times):
                self.logger.error("Thredds C10 start epoch time: %f out of bounds for thredds time array: %d."\
                % (start_epoch_time, self.c10_times[-1]))
            if closest_end_ndx == len(self.c10_times):
                self.logger.error("Thredds C10 end epoch time: %f out of bounds for thredds time array: %d."\
                % (end_epoch_time, self.c10_times[-1]))
      if self.logger:
        self.logger.debug("Thredds C10 Start: %s Avg Salinity: %s (%s,%s) Avg Water Temp: %s (%s,%s)"\
                          % (start_date.strftime('%Y-%m-%d %H:%M:%S'),
                             str(wq_tests_data['c10_avg_salinity_24']), str(wq_tests_data['c10_min_salinity_24']), str(wq_tests_data['c10_max_salinity_24']),
                             str(wq_tests_data['c10_avg_water_temp_24']), str(wq_tests_data['c10_min_water_temp']), str(wq_tests_data['c10_max_water_temp'])))
    if self.logger:
      self.logger.debug("Finished thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    return

  def get_nws_data(self, start_date, wq_tests_data):
    #IF the start date is more than 30 days ago, we have to go to an archival
    #source for the wind data as the xenia database only keeps the past 30 days on hand.
    #NWS only keeps the past 7 days available, so you've got to got to NGDC to get the data
    #to import into the archival database.
    if start_date < timezone('UTC').localize(datetime.utcnow()) - timedelta(days=30):
      florida_wq_historical_data.get_nws_data(self, start_date, wq_tests_data)
    else:
      platform_handle = 'nws.KSRQ.metar'

      if self.logger:
        self.logger.debug("Start retrieving nws platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      #Get the sensor id for wind speed and wind direction
      wind_spd_sensor_id = self.xenia_obs_db.sensorExists('wind_speed', 'm_s-1', platform_handle, 1)
      wind_dir_sensor_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      try:
        wind_speed_data = self.xenia_obs_db.session.query(multi_obs)\
          .filter(multi_obs.sensor_id == wind_spd_sensor_id)\
          .filter(multi_obs.platform_handle.ilike(platform_handle))\
          .filter(multi_obs.m_date >= begin_date)\
          .filter(multi_obs.m_date < end_date)\
          .order_by(multi_obs.m_date).all()

        wind_dir_data = self.xenia_obs_db.session.query(multi_obs)\
          .filter(multi_obs.sensor_id == wind_dir_sensor_id)\
          .filter(multi_obs.platform_handle.ilike(platform_handle))\
          .filter(multi_obs.m_date >= begin_date)\
          .filter(multi_obs.m_date < end_date)\
          .order_by(multi_obs.m_date).all()
      except Exception, e:
        if self.logger:
          self.logger.exception(e)
      else:
        wind_dir_tuples = []
        direction_tuples = []
        scalar_speed_avg = None
        speed_count = 0
        for wind_speed_row in wind_speed_data:
          for wind_dir_row in wind_dir_data:
            if wind_speed_row.m_date == wind_dir_row.m_date:
              if self.logger:
                self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
              if scalar_speed_avg is None:
                scalar_speed_avg = 0
              scalar_speed_avg += wind_speed_row.m_value
              speed_count += 1
              #Vector using both speed and direction.
              wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
              #Vector with speed as constant(1), and direction.
              direction_tuples.append((1, wind_dir_row.m_value))
              break

        avg_speed_dir_components = calcAvgSpeedAndDir(wind_dir_tuples)
        if self.logger:
          self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                            avg_speed_dir_components[0],
                                                                                            avg_speed_dir_components[0] * meters_per_second_to_mph,
                                                                                            avg_speed_dir_components[1]))

        #Unity components, just direction with speeds all 1.
        avg_dir_components = calcAvgSpeedAndDir(direction_tuples)
        scalar_speed_avg = scalar_speed_avg / speed_count
        wq_tests_data['nws_ksrq_avg_wspd'] = scalar_speed_avg * meters_per_second_to_mph
        wq_tests_data['nws_ksrq_avg_wdir'] = avg_dir_components[1]
        if self.logger:
          self.logger.debug("Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                   scalar_speed_avg,
                                                                                                   scalar_speed_avg * meters_per_second_to_mph,
                                                                                                   avg_dir_components[1]))

      if self.logger:
        self.logger.debug("Finished retrieving nws platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

    return