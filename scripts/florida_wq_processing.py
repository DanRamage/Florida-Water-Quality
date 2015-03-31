import sys
sys.path.append('../commonfiles')

import logging.config
from datetime import datetime, timedelta
from pytz import timezone
import optparse
import ConfigParser
import csv

import netCDF4 as nc
from netcdftime import DateFromJulianDay
import numpy as np
from math import isnan
from bisect import bisect_left

from collections import OrderedDict
from shapely.geometry import Polygon

from wqHistoricalData import wq_data
from wqXMRGProcessing import wqDB
from wqHistoricalData import station_geometry,sampling_sites, wq_defines, geometry_list, tide_data_file
from date_time_utils import get_utc_epoch
from NOAATideData import noaaTideData
from romsTools import bbox2ij, closestCellFromPtInPolygon

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import path

"""
florida_wq_data
Class is responsible for retrieving the data used for the sample sites models.
"""
class florida_wq_data(wq_data):
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
    self.c10_water_temp = self.ncObj.variables['temperature_04'][:]
    self.c10_salinity = self.ncObj.variables['salinity_04'][:]

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

    """
    if self.logger:
      self.logger.debug("Connecting to thredds endpoint for ncsu data: %s" % (kwargs['ncsu_model_tds_url']))
    self.ncsu_model = nc.Dataset(kwargs['ncsu_model_tds_url'])

    self.ncsu_model_time = self.ncsu_model.variables['time3'][:]
    model_bbox = [float(self.model_bbox[0]),float(self.model_bbox[2]),
                          float(self.model_bbox[1]),float(self.model_bbox[3])]

    #Determine the bounding box indexes.
    lons = self.ncsu_model.variables['lon_rho'][:]
    lats = self.ncsu_model.variables['lat_rho'][:]
    # latitude lower and upper index
    #self.ncsu_latli = np.argmin( np.abs( lats - model_bbox[2] ) )
    #self.ncsu_latui = np.argmin( np.abs( lats - model_bbox[3] ) )
    # longitude lower and upper index
    #self.ncsu_lonli = np.argmin( np.abs( lons - model_bbox[0] ) )
    #self.ncsu_lonui = np.argmin( np.abs( lons - model_bbox[1] ) )
    self.ncsu_i0,self.ncsu_i1,self.ncsu_j0,self.ncsu_j1 = bbox2ij(lons,lats,model_bbox)

    self.ncsu_lon_array = self.ncsu_model.variables['lon_rho'][self.ncsu_j0:self.ncsu_j1,self.ncsu_i0:self.ncsu_i1]
    self.ncsu_lat_array = self.ncsu_model.variables['lat_rho'][self.ncsu_j0:self.ncsu_j1,self.ncsu_i0:self.ncsu_i1]
    with open("/Users/danramage/tmp/salinity_model.csv", "w") as out_file:
      out_file.write("Latitude,Longitude,Lat Ndx,Lon Ndx\n")
      #flat_lon = self.ncsu_lon_array.flatten()
      #flat_lat = self.ncsu_lat_array.flatten()
      for lon_ndx in range(0,len(self.ncsu_lon_array[0])):
        for lat_ndx in range(0,len(self.ncsu_lat_array[0])):
          out_file.write("%f,%f,%d,%d\n" % (self.ncsu_lat_array[0][lat_ndx],self. ncsu_lon_array[0][lon_ndx], lat_ndx, lon_ndx))
    """
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

    self.get_nws_data(start_date, wq_tests_data)
    self.get_c10_data(start_date, wq_tests_data)
    self.get_nexrad_data(start_date, wq_tests_data)
    self.get_tide_data(start_date, wq_tests_data)
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
    var_name = '%s_tide_range' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = '%s_tide_hi' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = '%s_tide_lo' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    #Build variables for the subordinate tide station.
    var_name = '%s_tide_range' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = '%s_tide_hi' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = '%s_tide_lo' % (self.tide_offset_settings['tide_station'])
    wq_tests_data[var_name] = wq_defines.NO_DATA

    wq_tests_data['nws_ksrq_avg_wspd'] = wq_defines.NO_DATA
    wq_tests_data['nws_ksrq_avg_wdir'] = wq_defines.NO_DATA

    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 192, 24):
          var_name = '%s_summary_%d' % (boundary.name.lower().replace(' ', '_'), prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_dry_days_count' % (boundary.name.lower().replace(' ', '_'))
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_rainfall_intesity' % (boundary.name.lower().replace(' ', '_'))
        wq_tests_data[var_name] = wq_defines.NO_DATA

    wq_tests_data['c10_avg_salinity_24'] = wq_defines.NO_DATA
    #wq_tests_data['c10_salinity_rec_cnt'] = wq_defines.NO_DATA
    wq_tests_data['c10_min_salinity'] = wq_defines.NO_DATA
    wq_tests_data['c10_max_salinity'] = wq_defines.NO_DATA
    wq_tests_data['c10_avg_water_temp_24'] = wq_defines.NO_DATA
    #wq_tests_data['c10_water_temp_rec_cnt'] = wq_defines.NO_DATA
    wq_tests_data['c10_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['c10_max_water_temp'] = wq_defines.NO_DATA

    wq_tests_data['hycom_avg_salinity_24'] = wq_defines.NO_DATA
    wq_tests_data['hycom_min_salinity'] = wq_defines.NO_DATA
    wq_tests_data['hycom_max_salinity'] = wq_defines.NO_DATA
    wq_tests_data['hycom_avg_water_temp_24'] = wq_defines.NO_DATA
    wq_tests_data['hycom_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['hycom_max_water_temp'] = wq_defines.NO_DATA

    wq_tests_data['ncsu_avg_salinity_24'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_min_salinity'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_max_salinity'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_avg_water_temp_24'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_min_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['ncsu_max_water_temp'] = wq_defines.NO_DATA
    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return

  def get_ncsu_model_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving ncsu model data: %s" % (start_date))


    beginning_time = timezone('UTC').localize(datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S'))
    begin_date = start_date - timedelta(hours=24)
    end_date = start_date
    #Verify that the date we are interested should be in the ncsu model data.
    if begin_date >= (beginning_time + timedelta(days=self.ncsu_model_time[0])):
      start_time_delta = begin_date - beginning_time
      end_time_delta = end_date - beginning_time

      #The time dimension in the model is hours offset since the beginning_time above.
      #offset_hours = (time_delta.days * 24) + (time_delta.seconds / (60 * 60 * 24))
      offset_start = (start_time_delta.days) + (start_time_delta.seconds / (60.0 * 60.0 * 24.0))
      offset_end = (end_time_delta.days) + (end_time_delta.seconds / (60.0 * 60.0 * 24.0))
      closest_start_ndx = bisect_left(self.ncsu_model_time, offset_start)
      closest_end_ndx = bisect_left(self.ncsu_model_time, offset_end)
      if closest_start_ndx != -1 and closest_end_ndx != -1:
        """
        with open("/Users/danramage/tmp/salinity_model.csv", "w") as out_file:
          out_file.write("Latitude,Longitude,Lat Ndx,Lon Ndx\n")
          for lon_ndx in range(0,len(lon_array)):
            for lat_ndx in range(0,len(lat_array)):
              out_file.write("%f,%f,%d,%d\n" % (lat_array[lat_ndx], lon_array[lon_ndx], lat_ndx, lon_ndx))
        """
        try:
          salinity_data = self.ncsu_model.variables['salinity'][closest_start_ndx:closest_end_ndx,0,self.ncsu_latli:self.ncsu_latui,self.ncsu_lonli:self.ncsu_lonui]
          pt = closestCellFromPtInPolygon(self.site.object_geometry,
                                          self.ncsu_lon_array, self.ncsu_lat_array,
                                          salinity_data[0],
                                          self.ncsu_model.variables['salinity']._FillValue,
                                          self.ncsu_model_within_polygon)
          #Get the salinity data
          avg_salinity_pts = salinity_data[0:closest_end_ndx-closest_start_ndx,pt.y,pt.x]
          wq_tests_data['ncsu_avg_salinity_24'] = np.average(avg_salinity_pts)
          wq_tests_data['ncsu_min_salinity'] = avg_salinity_pts.min()
          wq_tests_data['ncsu_max_salinity'] = avg_salinity_pts.max()

          water_temp_data = self.ncsu_model.variables['temperature'][closest_start_ndx:closest_end_ndx,0,self.ncsu_latli:self.ncsu_latui,self.ncsu_lonli:self.ncsu_lonui]
          avg_water_temp_pts = water_temp_data[0:closest_end_ndx-closest_start_ndx,pt.y,pt.x]
          #Get the water temperature data
          wq_tests_data['ncsu_avg_water_temp_24'] = np.average(avg_water_temp_pts)
          wq_tests_data['ncsu_min_water_temp'] = avg_water_temp_pts.min()
          wq_tests_data['ncsu_max_water_temp'] = avg_water_temp_pts.max()
          if self.logger:
            cell_lat = self.ncsu_lat_array[pt.y]
            cell_lon = self.ncsu_lon_array[pt.x]
            begin_dt = beginning_time + timedelta(days=(self.ncsu_model_time[closest_start_ndx]))
            end_dt = beginning_time + timedelta(days=(self.ncsu_model_time[closest_end_ndx]))
            self.logger.debug("Site: %s Dates: %s to %s closest cell@ Lat: %f(%d) Lon: %f(%d) Salinity Avg: %f Water Temp Avg: %f"\
                              % (self.site.name,\
                                 begin_dt.strftime('%Y-%m-%dT%H:%M:%S'), end_dt.strftime('%Y-%m-%dT%H:%M:%S'),\
                                 cell_lat, pt.x, cell_lon, pt.y,\
                                 wq_tests_data['ncsu_avg_salinity_24'],wq_tests_data['ncsu_avg_water_temp_24']))

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
      self.logger.debug("Finished retrieving ncsu model data: %s" % (start_date))
    return

  def get_hycom_model_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving hycom model data: %s" % (start_date))


    beginning_time = timezone('UTC').localize(datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S'))
    begin_date = start_date - timedelta(hours=24)
    end_date = start_date
    #Verify that the date we are interested should be in the hycom model data.
    if begin_date >= (beginning_time + timedelta(days=self.hycom_model_time[0])):
      start_time_delta = begin_date - beginning_time
      end_time_delta = end_date - beginning_time

      #The time dimension in the model is hours offset since the beginning_time above.
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
          salinity_data = self.hycom_model.variables['salinity'][closest_start_ndx:closest_end_ndx,0,self.hycom_latli:self.hycom_latui,self.hycom_lonli:self.hycom_lonui]
          pt = closestCellFromPtInPolygon(self.site.object_geometry,
                                          self.hycom_lon_array, self.hycom_lat_array,
                                          salinity_data[0],
                                          self.hycom_model.variables['salinity']._FillValue,
                                          self.model_within_polygon)
          #Get the salinity data
          avg_salinity_pts = salinity_data[0:closest_end_ndx-closest_start_ndx,pt.y,pt.x]
          wq_tests_data['hycom_avg_salinity_24'] = np.average(avg_salinity_pts)
          wq_tests_data['hycom_min_salinity'] = avg_salinity_pts.min()
          wq_tests_data['hycom_max_salinity'] = avg_salinity_pts.max()

          if self.logger:
            self.logger.debug("Retrieving hycom water temp data.")
          water_temp_data = self.hycom_model.variables['temperature'][closest_start_ndx:closest_end_ndx,0,self.hycom_latli:self.hycom_latui,self.hycom_lonli:self.hycom_lonui]
          avg_water_temp_pts = water_temp_data[0:closest_end_ndx-closest_start_ndx,pt.y,pt.x]
          #Get the water temperature data
          wq_tests_data['hycom_avg_water_temp_24'] = np.average(avg_water_temp_pts)
          wq_tests_data['hycom_min_water_temp'] = avg_water_temp_pts.min()
          wq_tests_data['hycom_max_water_temp'] = avg_water_temp_pts.max()
          if self.logger:
            cell_lat = self.hycom_lat_array[pt.y]
            cell_lon = self.hycom_lon_array[pt.x]
            begin_dt = beginning_time + timedelta(days=(self.hycom_model_time[closest_start_ndx]))
            end_dt = beginning_time + timedelta(days=(self.hycom_model_time[closest_end_ndx]))
            self.logger.debug("Site: %s Dates: %s to %s closest cell@ Lat: %f(%d) Lon: %f(%d) Salinity Avg: %f Water Temp Avg: %f"\
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
        wq_tests_data['%s_tide_range' % (self.tide_station)] = tide_rec['range']
        wq_tests_data['%s_tide_hi' % (self.tide_station)] = tide_rec['hi']
        wq_tests_data['%s_tide_lo' % (self.tide_station)] = tide_rec['lo']
      #THe web service is unreliable, so if we were using the history csv file, it may still be missing
      #a record, if so, let's try to look it up on the web.
      else:
        use_web_service = True
    if self.tide_data_obj is None or use_web_service:
      tide = noaaTideData(use_raw=True, logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD

      try:
        tide_data = tide.calcTideRange(beginDate = start_date.strftime('%Y%m%d'),
                           endDate = start_date.strftime('%Y%m%d'),
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
            wq_tests_data['%s_tide_range' % (self.tide_station)] = range
            wq_tests_data['%s_tide_hi' % (self.tide_station)] = tide_data['HH']['value']
            wq_tests_data['%s_tide_lo' % (self.tide_station)] = tide_data['LL']['value']
        else:
          if self.logger:
            self.logger.error("Tide data for station: %s date: %s not available or only partial." % (self.tide_station, start_date))

    #Save subordinate station values
    if wq_tests_data['%s_tide_hi'%(self.tide_station)] != wq_defines.NO_DATA:
      offset_hi = wq_tests_data['%s_tide_hi'%(self.tide_station)] * self.tide_offset_settings['hi_tide_height_offset']
      offset_lo = wq_tests_data['%s_tide_lo'%(self.tide_station)] * self.tide_offset_settings['lo_tide_height_offset']

      tide_station = self.tide_offset_settings['tide_station']
      wq_tests_data['%s_tide_range' % (tide_station)] = offset_hi - offset_lo
      wq_tests_data['%s_tide_hi' % (tide_station)] = offset_hi
      wq_tests_data['%s_tide_lo' % (tide_station)] = offset_lo

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
      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      if self.logger:
        self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 192, 24):
        var_name = '%s_summary_%d' % (boundary.name.lower().replace(' ', '_'), prev_hours)
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
      prev_dry_days = self.xenia_db.getPrecedingRadarDryDaysCount(platform_handle,
                                             start_date,
                                             'precipitation_radar_weighted_average',
                                             'mm')
      if prev_dry_days is not None:
        var_name = '%s_dry_days_count' % (boundary.name.lower().replace(' ', '_'))
        wq_tests_data[var_name] = prev_dry_days

      rainfall_intensity = self.xenia_db.calcRadarRainfallIntensity(platform_handle,
                                                               start_date,
                                                               60,
                                                              'precipitation_radar_weighted_average',
                                                              'mm')
      if rainfall_intensity is not None:
        var_name = '%s_rainfall_intesity' % (boundary.name.lower().replace(' ', '_'))
        wq_tests_data[var_name] = rainfall_intensity

      if self.logger:
        self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))

    if self.logger:
      self.logger.debug("Finished retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))


  def get_c10_data(self, start_date, wq_tests_data):
    start_epoch_time = int(get_utc_epoch(start_date) + 0.5)

    #Find the starting time index to work from.
    if self.logger:
      self.logger.debug("Start thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))


    closest_start_time_idx = bisect_left(self.c10_times, start_epoch_time)
    c10_val = int(self.c10_times[closest_start_time_idx-1] + 0.5)
    if start_epoch_time == c10_val:
      closest_start_time_idx -= 1

    if (closest_start_time_idx != 0 and closest_start_time_idx != len(self.c10_times)) \
      and (closest_start_time_idx != len(self.c10_times)):

      closest_datetime = datetime.fromtimestamp(self.c10_times[closest_start_time_idx], timezone('UTC'))
      prev_hour_dt = closest_datetime - timedelta(hours=24)
      prev_hour_epoch = int(get_utc_epoch(prev_hour_dt) + 0.5)
      #Get closest index for date/time for our prev_hour interval.
      closest_prev_hour_time_idx = bisect_left(self.c10_times, prev_hour_epoch)

      if self.logger:
        self.logger.debug("Thredds C10 closest Starting datetime: %s Ending Datetime: %s"\
                          % (closest_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                             datetime.fromtimestamp(self.c10_times[closest_prev_hour_time_idx], timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')))
      #for prev_hours in range(24, 24, 24):

      rec_cnt = closest_start_time_idx-closest_prev_hour_time_idx
      if rec_cnt > 24 or rec_cnt < 12:
        if self.logger:
          self.logger.error("Thredds record counts are suspect: %d" % (rec_cnt))
      avg_c10_salinity = 0
      sal_rec_cnt = 0
      min_sal = None
      max_sal = None
      avg_c10_wt = 0
      temp_rec_cnt = 0
      min_temp = None
      max_temp = None
      for ndx in range(closest_prev_hour_time_idx, closest_start_time_idx):
        if not isnan(self.c10_salinity[ndx]):
          avg_c10_salinity += self.c10_salinity[ndx]
          if min_sal is None or min_sal > self.c10_salinity[ndx]:
            min_sal = self.c10_salinity[ndx]
          if max_sal is None or max_sal < self.c10_salinity[ndx]:
            max_sal = self.c10_salinity[ndx]
          sal_rec_cnt += 1
        if not isnan(self.c10_water_temp[ndx]):
          avg_c10_wt += self.c10_water_temp[ndx]
          if min_temp is None or min_temp > self.c10_water_temp[ndx]:
            min_temp = self.c10_water_temp[ndx]
          if max_sal is None or max_temp < self.c10_water_temp[ndx]:
            max_temp = self.c10_water_temp[ndx]
          temp_rec_cnt += 1
      if sal_rec_cnt > 0:
        avg_c10_salinity = avg_c10_salinity / sal_rec_cnt
        wq_tests_data['c10_avg_salinity_24'] = avg_c10_salinity
        #wq_tests_data['c10_salinity_rec_cnt'] = sal_rec_cnt
        wq_tests_data['c10_min_salinity'] = min_sal
        wq_tests_data['c10_max_salinity'] = max_sal

      if temp_rec_cnt > 0:
        avg_c10_wt = avg_c10_wt / temp_rec_cnt
        wq_tests_data['c10_avg_water_temp_24'] = avg_c10_wt
        #wq_tests_data['c10_water_temp_rec_cnt'] = temp_rec_cnt
        wq_tests_data['c10_min_water_temp'] = min_temp
        wq_tests_data['c10_max_water_temp'] = max_temp

        if self.logger:
          self.logger.debug("Thredds C10 Start: %s End: %s Avg Salinity: %s (%s,%s) Avg Water Temp: %s (%s,%s)"\
                            % (start_date.strftime('%Y-%m-%d %H:%M:%S'), prev_hour_dt.strftime('%Y-%m-%d %H:%M:%S'),
                               str(avg_c10_salinity), str(min_sal), str(max_sal),
                               str(avg_c10_wt), str(min_temp), str(max_temp)))
    if self.logger:
      self.logger.debug("Finished thredds C10 search for datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

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
        header_row = ["WKT","EPAbeachID","SPLocation","Boundary"]
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
              station = station_geometry(row['SPLocation'], row['WKT'])
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

    return False


def create_historical_summary(config_file_name,
                              historical_wq_file,
                              header_row,
                              summary_out_file,
                              starting_date,
                              start_time_midnight,
                              tide_csv_file,
                              use_logger=False):
  logger = None
  if use_logger:
    logger = logging.getLogger('create_historical_summary_logger')
  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(config_file_name)

    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    xenia_db_file = config_file.get('database', 'name')
    c_10_tds_url = config_file.get('c10_data', 'historical_qaqc_thredds')
    hycom_model_tds_url = config_file.get('hycom_model_data', 'thredds_url')
    model_bbox = config_file.get('hycom_model_data', 'bbox').split(',')
    poly_parts = config_file.get('hycom_model_data', 'within_polygon').split(';')
    model_within_polygon = [(float(lon_lat.split(',')[0]), float(lon_lat.split(',')[1])) for lon_lat in poly_parts]

    ncsu_model_tds_url = config_file.get('ncsu_model_data', 'thredds_url')

  except ConfigParser, e:
    if logger:
      logger.exception(e)
  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    fl_sites = florida_sample_sites(True)
    fl_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    #If we provide a tide file that has the historical data, we'll load it instead of using
    #the SOAP webservice.
    tide_file = None
    if len(tide_csv_file):
      tide_file = tide_data_file(logger=True)
      tide_file.open(tide_csv_file)
    try:
      wq_file = open(historical_wq_file, "rU")
      wq_history_file = csv.DictReader(wq_file, delimiter=',', quotechar='"', fieldnames=header_row)
    except IOError, e:
      if logger:
        logger.exception(e)
    else:
      line_num = 0
      #Dates in the spreadsheet are stored in EST. WE want to work internally in UTC.
      eastern = timezone('US/Eastern')

      output_keys = ['station_name', 'sample_date', 'enterococcus_value', 'enterococcus_code', 'autonumber', 'County']

      sites_not_found = []
      current_site = None
      processed_sites = []
      #stop_date = eastern.localize(datetime.strptime('2014-01-29 00:00:00', '%Y-%m-%d %H:%M:%S'))
      #stop_date = stop_date.astimezone(timezone('UTC'))
      try:
        fl_wq_data = florida_wq_data(xenia_database_name=xenia_db_file,
                                      c_10_tds_url=c_10_tds_url,
                                      hycom_model_tds_url=hycom_model_tds_url,
                                      ncsu_model_tds_url=ncsu_model_tds_url,
                                      model_bbox=model_bbox,
                                      model_within_polygon=model_within_polygon,
                                      use_logger=True)
      except Exception, e:
        if logger:
          logger.exception(e)
      else:
        if logger:
          logger.info("Begin looping through file: %s" % (historical_wq_file))
        for row in wq_history_file:
          #Check to see if the site is one we are using
          if line_num > 0:
            cleaned_site_name = row['SPLocation'].replace("  ", " ")

            date_val = row['Date']
            time_val = row['SampleTime']
            if len(date_val):
              #Date does not have leading 0s sometimes, so we add them.
              date_parts = date_val.split('/')
              date_val = "%02d/%02d/%02d" % (int(date_parts[0]), int(date_parts[1]), int(date_parts[2]))
              #If we want to use midnight as the starting time, or we did not have a sample time.
              if start_time_midnight or len(time_val) == 0:
                time_val = '00:00:00'
              #Time doesn't have leading 0's so add them
              else:
                hours_mins = time_val.split(':')
                time_val = "%02d:%02d:00" % (int(hours_mins[0]), int(hours_mins[1]))
              try:
                wq_date = eastern.localize(datetime.strptime('%s %s' % (date_val, time_val), '%m/%d/%y %H:%M:%S'))
              except ValueError, e:
                try:
                  wq_date = eastern.localize(datetime.strptime('%s %s' % (date_val, time_val), '%m/%d/%Y %H:%M:%S'))
                except ValueError, e:
                  if logger:
                    logger.error("Processing halted at line: %d" % (line_num))
                    logger.exception(e)
                  sys.exit(-1)
              #Convert to UTC
              wq_utc_date = wq_date.astimezone(timezone('UTC'))

              if (starting_date is not None and wq_utc_date >= starting_date) or (starting_date is None):
                if fl_sites.get_site(cleaned_site_name):
                  new_outfile = False
                  if current_site != cleaned_site_name:
                    #Initialize site name
                    if current_site != None:
                      site_data_file.close()

                    current_site = cleaned_site_name
                    append_file = False
                    if current_site in processed_sites:
                      if logger:
                        logger.debug("Site: %s has been found again, data is not ordered.")
                      append_file = True
                    else:
                      processed_sites.append(current_site)
                    #We need to create a new data access object using the boundaries for the station.
                    site = fl_sites.get_site(cleaned_site_name)
                    try:
                      #Get the station specific tide stations
                      tide_station = config_file.get(cleaned_site_name, 'tide_station')
                      offset_tide_station = config_file.get(cleaned_site_name, 'offset_tide_station')
                      tide_offset_settings = {
                        'tide_station': config_file.get(offset_tide_station, 'station_id'),
                        'hi_tide_time_offset': config_file.getint(offset_tide_station, 'hi_tide_time_offset'),
                        'lo_tide_time_offset': config_file.getint(offset_tide_station, 'lo_tide_time_offset'),
                        'hi_tide_height_offset': config_file.getfloat(offset_tide_station, 'hi_tide_height_offset'),
                        'lo_tide_height_offset': config_file.getfloat(offset_tide_station, 'lo_tide_height_offset')
                      }

                    except ConfigParser.Error, e:
                      if logger:
                        logger.exception(e)

                    fl_wq_data.reset(site=site,
                                      tide_station=tide_station,
                                      tide_offset_params=tide_offset_settings,
                                      tide_data_obj=tide_file)

                    clean_filename = cleaned_site_name.replace(' ', '_')
                    sample_site_filename = "%s/%s-Historical.csv" % (summary_out_file, clean_filename)
                    write_header = True
                    try:
                      if not append_file:
                        if logger:
                          logger.debug("Opening sample site history file: %s" % (sample_site_filename))
                        site_data_file = open(sample_site_filename, 'w')
                      else:
                        if logger:
                          logger.debug("Opening sample site history file with append: %s" % (sample_site_filename))
                        site_data_file = open(sample_site_filename, 'a')
                        write_header = False
                    except IOError, e:
                      if logger:
                        logger.exception(e)
                      raise e
                  if logger:
                    logger.debug("Start building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))
                  site_data = OrderedDict([('autonumber', row['autonumber']),
                                           ('station_name',row['SPLocation']),
                                           ('sample_datetime', wq_date.strftime("%Y-%m-%d %H:%M:%S")),
                                           ('sample_datetime_utc', wq_utc_date.strftime("%Y-%m-%d %H:%M:%S")),
                                           ('County', row['County']),
                                           ('enterococcus_value', row['enterococcus']),
                                           ('enterococcus_code', row['enterococcus_code'])])
                  try:
                    fl_wq_data.query_data(wq_utc_date, wq_utc_date, site_data)
                  except Exception,e:
                    if logger:
                      logger.exception(e)
                    sys.exit(-1)
                  #wq_data_obj.append(site_data)
                  header_buf = []
                  data = []
                  for key in site_data:
                    if write_header:
                      header_buf.append(key)
                    if site_data[key] != wq_defines.NO_DATA:
                      data.append(str(site_data[key]))
                    else:
                      data.append("")
                  if write_header:
                    site_data_file.write(",".join(header_buf))
                    site_data_file.write('\n')
                    header_buf[:]
                    write_header = False

                  site_data_file.write(",".join(data))
                  site_data_file.write('\n')
                  site_data_file.flush()
                  data[:]
                  if logger:
                    logger.debug("Finished building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (row['SPLocation'], wq_utc_date, wq_date))



                else:
                  try:
                    sites_not_found.index(row['SPLocation'])
                  except ValueError,e:
                    sites_not_found.append(row['SPLocation'])

          line_num += 1
      wq_file.close()
      if logger:
        logger.debug("Stations not matching: %s" % (", ".join(sites_not_found)))


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportData", dest="import_data",
                    help="Directory to import XMRG files from" )
  parser.add_option("-b", "--BuildSummaryData",
                    action="store_true", default=True,
                    dest="build_summary_data",
                    help="Flag that specifies to construct summary file.")
  parser.add_option("-w", "--WaterQualityHistoricalFile", dest="historical_wq_file",
                    help="Input file with the dates and stations we are creating summary for." )
  parser.add_option("-r", "--HistoricalSummaryHeaderRow", dest="historical_file_header_row",
                    help="Input file with the dates and stations we are creating summary for." )
  parser.add_option("-s", "--HistoricalSummaryOutPath", dest="summary_out_path",
                    help="Directory to write the historical summary data to." )
  parser.add_option("-d", "--StartDate", dest="starting_date",
                    help="Date to use for the retrieval." )
  parser.add_option("-m", "--StartTimeMidnight", dest="start_time_midnight",
                    action="store_true", default=False,
                    help="Set time to 00:00:00 for the queries instead of the sample time." )
  parser.add_option("-t", "--TideDataFile", dest="tide_data_file",
                    help="If used, this is the path to a tide data csv file.", default=None )


  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.config_file)

    logger = None
    logConfFile = configFile.get('logging', 'xmrg_ingest')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger('florida_wq_processing_logger')
      logger.info("Log file opened.")
  except ConfigParser.Error, e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    starting_date = None
    if options.starting_date:
      starting_date = timezone('UTC').localize(datetime.strptime(options.starting_date, '%Y-%m-%dT%H:%M:%S'))
    if options.build_summary_data:
      create_historical_summary(options.config_file,
                                options.historical_wq_file,
                                options.historical_file_header_row.split(','),
                                options.summary_out_path,
                                starting_date,
                                options.start_time_midnight,
                                options.tide_data_file,
                                True)
  if logger:
    logger.info("Log file closed.")
  return


if __name__ == "__main__":
  main()
