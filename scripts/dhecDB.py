"""
Revisions:
Date: 2011-07-27
Functions: __init__
Changes: Added logger parameter to allow passing the logger object in.

Date: 2011-06-23
Function: getLastNHoursSummaryFromRadarPrecip
Changes: Now we are adding the weighted averages even for values of 0 into the database. Had to rework
this function to calculate the preceeding dry days.
"""
import logging
from pysqlite2 import dbapi2 as sqlite3
from xenia import xeniaSQLite


class nexrad_db(object):
  def __init__(self):
    self.db_connection = None

  def connect(self, **kwargs):
    if 'db_name' in kwargs:
      self.db_connection = sqlite3.connect(kwargs['db_name'])
    #This enables the ability to manipulate rows with the column name instead of an index.
    self.db_connection.row_factory = sqlite3.Row

    #Get the schema files that make up the database.
    for schema_file in kwargs['nexrad_schema_files']:
      full_path = "%s%s" % (kwargs['nexrad_schema_directory'], schema_file)
      with open(full_path, 'rt') as f:
        schema = f.read()
        self.db_connection.executescript(schema)

    self.db_connection.enable_load_extension(True)
    sql = 'SELECT load_extension("%s");' % (kwargs['spatialite_lib'])
    db_cursor = self.db_connection.cursor()
    db_cursor.execute(sql)

    if(db_cursor != None):
      return(True)
    return(False)

  def insert_precip_record(self, datetime, filetime, latitude, longitude, val, grid_polygon, trans_cursor=None):
    sql = "INSERT INTO precipitation_radar \
          (insert_date,collection_date,latitude,longitude,precipitation,geom) \
          VALUES('%s','%s',%f,%f,%f,GeomFromText('%s',4326));" \
          %(datetime, filetime, latitude, longitude, val, grid_polygon.wkt)
    if trans_cursor != None:
      trans_cursor.execute(sql)
    else:
      cursor = self.db_connection.cursor()
      cursor.execute(sql)
      cursor.close()

  def commit(self):
    self.db_connection.commit()

  def delete_all(self):
    sql = "DELETE FROM precipitation_radar;"
    cursor = self.db_connection.execute(sql);
    self.db_connection.commit()
    cursor.close()
    sql = "VACUUM;"
    cursor = self.db_connection.execute(sql);
    cursor.close()

  def close(self):
    self.db_connection.close()

  """
  Function: get_radar_data_for_boundary
  Purpose: For a given polygon this function queries the radar data, gets the grids that fall
   into the polygon of interest.
  Parameters:
    boundary_polygon is the shapely Polygon we want to query the intersecting grids for.
    start_time is the starting time in YYYY-MM-DDTHH:MM:SS format.
    end_time is the starting time in YYYY-MM-DDTHH:MM:SS format.
  Return:
    Cursor with the recordset or None if query failed.
  """
  def get_radar_data_for_boundary(self, boundary_polygon, start_time, end_time):
    sql = "SELECT ogc_fid,latitude,longitude,precipitation,AsText(geom) as WKT FROM precipitation_radar \
            WHERE\
            (collection_date >= '%s' AND collection_date <= '%s') AND\
            Intersects( Geom, \
                        GeomFromText('%s'))"\
            % (start_time, end_time, boundary_polygon.wkt)
    db_cursor = self.db_connection.cursor()
    db_cursor.execute(sql)
    return db_cursor
  """
  Function: calculate_weighted_average
  Purpose: For a given polygon this function queries the radar data, gets the grids that fall
   into the watershed of interest and calculates the weighted average.
  Parameters:
    watershedName is the watershed we want to calculate the average for. For ease of use, I use the rain gauge name to
       name the watersheds.
    start_time is the starting time in YYYY-MM-DDTHH:MM:SS format.
    end_time is the starting time in YYYY-MM-DDTHH:MM:SS format.
  Return:
  Weighted average if calculated, otherwise -9999.
  """
  def calculate_weighted_average(self, boundary_polygon, start_time, end_time):
    weighted_avg = -9999
    #Get the percentages that the intersecting radar grid make up of the watershed boundary.
    sql = "SELECT * FROM(\
           SELECT (Area(Intersection(radar.geom,GeomFromText('%s')))/Area(GeomFromText('%s'))) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar\
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                Intersects(radar.geom, GeomFromText('%s')))"\
                %(boundary_polygon.wkt, boundary_polygon.wkt, start_time, end_time, boundary_polygon.wkt)
    db_cursor = self.db_connection.cursor()
    db_cursor.execute(sql)
    if db_cursor != None:
      total = 0.0
      date = ''
      cnt = 0
      for row in db_cursor:
        percent = row['percent']
        precip = row['precipitation']
        total += (percent * precip)
        cnt += 1
      db_cursor.close()
      if(cnt > 0):
        weighted_avg = total
    else:
      weighted_avg = None
    return(weighted_avg)


"""
Class: dhecDB
Purpose: Interface to the dhec beach advisory prediction database.
"""
class dhecDB(xeniaSQLite):
  """
  Function: __init__
  Purpose: Initializes the database object. Connects to the database passed in on the dbName parameter.
  """
  def __init__(self, dbName, loggerName=None, logger=None):
    xeniaSQLite.__init__(self)
    self.logger = None
    if(loggerName != None):
      self.logger = logging.getLogger(loggerName)
      self.logger.info("creating an instance of dhecDB")
    elif(logger != None):
      self.logger = logger
    self.totalRowsProcd = 0
    self.rowErrorCnt = 0
    self.lastErrorMsg = None
    if(xeniaSQLite.connect(self, dbName) == False):
      if(self.logger != None):
        self.logger.error(self.lastErrorMsg)
      sys.exit(-1)
  def __del__(self):
    self.DB.close()
  
  
  """
  Function: vacuumDB
  Purpose: Cleanup the database. 
  Parameters: None
  Return: True if successful, otherwise False.
  """    
  def vacuumDB(self):
    try:
      sql = "VACUUM;"
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)    
      return(True)    
    except sqlite3.Error, e:        
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)      
    except Exception, E:
      msg = self.procTraceback()
      self.logger.critical(msg)      
      sys.exit(-1)      
    return(False)

    
  """
  Function: writePrecip
  Purpose: Writes an entry into the precipitation table. Normally this is what we do when we
   are parsing the rain gauge data file to.
  Parameters: 
    dateTime is the date and time for the entry
    rain_gauge is the name of the rain gauge the entry is for
    batt_voltage is the battery voltage of the rain gauge
    program_code is teh program code for the rain gauge
    rainfall is the rainfall amount for
  Return: True if successful otherwise false. 
  """
  def writePrecip(self, dateTime, rain_gauge, batt_voltage, program_code, rainfall, wind=None, windDir=None):
    
    #Get the platform info so we can get the lat/long for the sensor.
    platformHandle = "dhec.%s.raingauge" % (rain_gauge)
    platformCursor = xeniaSQLite.getPlatformInfo(self, platformHandle)
    if(platformCursor != None):
      nfo = platformCursor.fetchone()
      if(nfo != None):
        mVals = []
        mVals.append(rainfall)
        mVals.append(batt_voltage)
        mVals.append(program_code)                                 
        if(xeniaSQLite.addMeasurement(self,
                                   'precipitation', 'in',
                                   platformHandle,
                                   dateTime,
                                   nfo['fixed_latitude'], nfo['fixed_longitude'],
                                   0,
                                   mVals,
                                   1,
                                   False) != True):
          self.logger.error( "%s"\
                             %(self.getErrorInfo()) )
          xeniaSQLite.clearErrorInfo(self)
          return(False)
        
        if(wind != None):
          mVals = []
          mVals.append(wind)
          if(xeniaSQLite.addMeasurement(self,
                                     'wind_speed', 'mph',
                                     platformHandle,
                                     dateTime,
                                     nfo['fixed_latitude'], nfo['fixed_longitude'],
                                     0,
                                     mVals,
                                     1,
                                     False) != True):           
            self.logger.error( "%s"\
                             %(self.getErrorInfo()) )
            xeniaSQLite.clearErrorInfo(self)
            return(False)
        if(windDir != None):    
          mVals.append(windDir)
          if(xeniaSQLite.addMeasurement(self,
                                     'wind_from_direction', 'degrees_true',
                                     platformHandle,
                                     dateTime,
                                     nfo['fixed_latitude'], nfo['fixed_longitude'],
                                     0,
                                     mVals,
                                     1,
                                     False) != True):
            self.logger.error( "%s"\
                             %(self.getErrorInfo()) )
            xeniaSQLite.clearErrorInfo(self)
            return(False)
      else:
        self.logger.error( "Platform: %s not found. Cannot add measurement." %(platformHandle) )
    else:
      self.logger.error( "%s" %(self.getErrorInfo()) )
      
    return(True)  
  """
  Function: write24HourSummary
  Purpose: Writes an entry into the precip_daily_summary table. This is the days summary for the rain gauge.
  Parameters: 
    dateTime is the date and time for the entry
    rain_gauge is the name of the rain gauge the entry is for
    rainfall is the rainfall amount for
  Return: True if successful otherwise false. 
  """
  def write24HourSummary(self, dateTime, rain_gauge, rainfall):
    #Get the platform info so we can get the lat/long for the sensor.
    platformHandle = "dhec.%s.raingauge" % (rain_gauge)
    platformCursor = xeniaSQLite.getPlatformInfo(self, platformHandle)
    if(platformCursor != None):
      nfo = platformCursor.fetchone()
      if(nfo != None):
        mVals = []
        mVals.append(rainfall)
        if(xeniaSQLite.addMeasurement(self,
                                   'precipitation_accumulated_daily', 'in',
                                   platformHandle,
                                   dateTime,
                                   nfo['fixed_latitude'], nfo['fixed_longitude'],
                                   0,
                                   mVals,
                                   1,
                                   True) != True):
          self.logger.error( "%s Function: %s Line: %s File: %s"\
                           %(self.lastErrorMsg,self.lastErrorFunc, self.lastErrorLineNo, self.lastErrorFile) )
          xeniaSQLite.clearErrorInfo(self)
          return(False)

    
  """
  Function: getInspectionDates
  Purpose: Queries the dhec_beach table and returns the dates of the inspections for the given
    station.
  Parameters: 
    station is the name of the station we are quering the dates for.
  Return: A list of the dates. If none were found, the list is empty. 
  """
  def getInspectionDates(self, station, whereClause="", convertToUTM=False):
    dateList = []
    where = ''
    if(len(whereClause)):
      where = "AND %s " %(whereClause)
    sql = "SELECT insp_date,insp_time FROM dhec_beach WHERE station = '%s' %s ORDER BY insp_date ASC"\
         % (station, where)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      for row in dbCursor:
        timeVal = int(row['insp_time']) 
        #insp_time is in a hhmm format, so we break it apart.
        hour = timeVal / 100    #Get the hours
        minute = timeVal - (hour * 100) # Get the minutes
        #The dates are EST, the data in the multi_obs table is in UTM, so if we want to convert
        #do it here.
        date = "%sT%02d:%02d:00" % (row['insp_date'], hour, minute)
        if(convertToUTM):
          date = time.strptime(date, "%Y-%m-%dT%H:%M:00")
          date = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.mktime(date)))          
        dateList.append(date)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.critical("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))      
      sys.exit(- 1)
    return(dateList)
  """
  Function: getMoonIllumination
  Purpose: For the given day, return the moon illumination(percentage of moon visible).
  Parameters:
    date is the day of interest
  Return:
    a floating point number representing the percentage of moon visible
  """
  def getMoonIllumination(self, date):
    moonPhase = -9999.0
    sql = "SELECT phase FROM moon_phase WHERE date = strftime( '%%Y-%%m-%%d','%s')" % (date)
    try:
      dbCursor = self.executeQuery(sql)
      if(dbCursor != None):
        val = dbCursor.fetchone()
        if(val != None):
          if(val['phase'] != None):
            moonPhase = float(val['phase'])
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.critical("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: \"%s\" Terminating script!" % (e.args[0], sql))      
      sys.exit(- 1)
      
    return(moonPhase)
  
  """
  Function: writeSummaryForStation
  Purpose:  Writes the entry for day into the summary table. 
  Parameters:
    sampleDate is the date in the dhec_beach table which is when the sample was taken.
    sampleTime is the date in th edhec_beach table when the sample was taken.
    station is the name of the station we are quering the dates for. 
  Return: A list of the dates. If none were found, the list is empty. 
  
  """
  def writeSummaryForStation(self, sampleDate, sampleTime, station, commit=True):
    #import dateTime
    
    sql = "SELECT  dhec_beach.station,dhec_beach.insp_type,dhec_beach.etcoc,dhec_beach.tide,dhec_beach.salinity,dhec_beach.weather,\
          monitoring_stations.rain_gauge\
          FROM dhec_beach,monitoring_stations \
          WHERE \
          monitoring_stations.station = '%s' AND \
          dhec_beach.station = '%s' AND \
          dhec_beach.insp_date = strftime('%%Y-%%m-%%d', '%s' ) AND\
          dhec_beach.insp_time = '%s'"\
          % (station, station, sampleDate, sampleTime)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      beachData = dbCursor.fetchone()
      
      #Build the dateTime used to query the other tables. The sampleDate and sampleTime are in
      #EST so we also convert over to UTC.
      sampleTime = int(sampleTime)
      hour = sampleTime / 100;
      minute = sampleTime - (hour * 100) # Get the minutes                    
      estDatetime = "%sT%02d:%02d:00" %(sampleDate, hour, minute)
      dateTime = estDatetime
      dataEpochTime = time.mktime(time.strptime(dateTime, '%Y-%m-%dT%H:%M:%S'))
      dateTime = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(dataEpochTime))
      if(beachData != None):
        rainGauge = beachData['rain_gauge']
        if(rainGauge != None):
          if(dateTime == '2009-12-31T16:25:00' and station == 'WAC-005A'):
            stop = 1
          #Query the rainfall totals over the given hours range. 
          #Get the last 24 hours summary
          sum24 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 24)
          if(sum24 == -9999.0):
            sum24 = 'NULL'
          #Get the last 48 hours summary
          sum48 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 48)
          if(sum48 == -9999.0):
            sum48 = 'NULL'
          #Get the last 72 hours summary
          sum72 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 72)
          if(sum72 == -9999.0):
            sum72 = 'NULL'
          #Get the last 96 hours summary
          sum96 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 96)
          if(sum96 == -9999.0):
            sum96 = 'NULL'
          #Get the last 120 hours summary
          sum120 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 120)
          if(sum120 == -9999.0):
            sum120 = 'NULL'
          #Get the last 144 hours summary
          sum144 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 144)
          if(sum144 == -9999.0):
            sum144 = 'NULL'
          #Get the last 168 hours summary
          sum168 = self.getLastNHoursSummaryFromPrecipSummary(dateTime, rainGauge, 168)
          if(sum168 == -9999.0):
            sum168 = 'NULL'
          #calculate the X day delay totals
          #1 day delay
          sum1daydelay = 'NULL'
          if(sum24 != 'NULL' and sum48 != 'NULL'):
            sum1daydelay = str(sum48 - sum24)
          #2 day delay
          sum2daydelay = 'NULL'
          if(sum72 != 'NULL' and sum48 != 'NULL'):
            sum2daydelay = str(sum72 - sum48)
          #3 day delay
          sum3daydelay = 'NULL'
          if(sum72 != 'NULL' and sum96 != 'NULL'):
            sum3daydelay = str(sum96 - sum72)
          #Now convert the values to strings. We use strings in the SQL call so we can use 'NULL' for values we don't have.
          sum24 = str(sum24)
          sum48 = str(sum48)
          sum72 = str(sum72)
          sum96 = str(sum96)
          sum120 = str(sum120)
          sum144 = str(sum144)
          sum168 = str(sum168)
          
          #Get the preceding dry days count, if there are any.
          dryCnt = self.getPrecedingDryDaysCount(dateTime, rainGauge)
          if(dryCnt == -9999):
            dryCnt = 'NULL'
          else:
            dryCnt = str(dryCnt)
          #Get the 24 hour rainfall intensity
          rainfallIntensity = self.calcRainfallIntensity(rainGauge, dateTime, 10)
          if(rainfallIntensity == -9999.0):
            rainfallIntensity = 'NULL'
          else:
            rainfallIntensity = str(rainfallIntensity)
                  
          #Write the summary table
          etcoc = 'NULL'
          if(beachData['etcoc'] != None and beachData['etcoc'] != ''):
            etcoc = str(beachData['etcoc'])
          salinity = 'NULL'
          if(beachData['salinity'] != None and beachData['salinity'] != ''):
            salinity = str(beachData['salinity'])
          tide = 'NULL'
          if(beachData['tide'] != None and beachData['tide'] != ''):
            tide = str(beachData['tide'])
          if(tide == 'NULL'):
            tide = self.getTideLevel(8661070, dateTime)
            if(tide == -9999):
              tide = 'NULL'
            else:
              tide = str(tide)
          weather = 'NULL'
          if(beachData['weather'] != None and beachData['weather'] != ''):
            weather = str(beachData['weather'])
          
          #Calculate the moon illumination. We want to calculate the illumination in EST at noon.
          
          moonCalcs = moon()
          #We use the sampleDate for the moon illumination since the dates are in EST in the table.
          #moon = self.getMoonIllumination(sampleDate)  
          if(moon == -9999.0):
            moon = 'NULL'
          else:
            moon = str(moon)
         
          
          #Query the rainfall totals over the given hours range. 
          #Get the last 24 hours summary
          radarSum24 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 24)
          if(radarSum24 == -9999.0):
            radarSum24 = 'NULL'
          #Get the last 48 hours summary
          radarSum48 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 48)
          if(radarSum48 == -9999.0):
            radarSum48 = 'NULL'
          #Get the last 72 hours summary
          radarSum72 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 72)
          if(radarSum72 == -9999.0):
            radarSum72 = 'NULL'
          #Get the last 96 hours summary
          radarSum96 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 96)
          if(radarSum96 == -9999.0):
            radarSum96 = 'NULL'
          #Get the last 120 hours summary
          radarSum120 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 120)
          if(radarSum120 == -9999.0):
            radarSum120 = 'NULL'
          #Get the last 144 hours summary
          radarSum144 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 144)
          if(radarSum144 == -9999.0):
            radarSum144 = 'NULL'
          #Get the last 168 hours summary
          radarSum168 = self.getLastNHoursSummaryFromRadarPrecip(dateTime, rainGauge, 168)
          if(radarSum168 == -9999.0):
            radarSum168 = 'NULL'
      
          radarIntensity = self.calcRadarRainfallIntensity( rainGauge, dateTime, 60)
          if(radarSum24 == 'NULL' or radarIntensity == -9999):
            radarIntensity = 'NULL'
          else:
            radarIntensity = str(radarIntensity)
          
          radarDryCnt = 'NULL'
          if(radarSum24 != 'NULL'):
            radarDryCnt = self.getPrecedingRadarDryDaysCount(dateTime, rainGauge)
            if(radarDryCnt == -9999):
              radarDryCnt = 'NULL'
            else:
              radarDryCnt = str(radarDryCnt)
          
          #calculate the X day delay totals
          #1 day delay
          radarsum1daydelay = 'NULL'
          if(radarSum48 != 'NULL' and radarSum24 != 'NULL'):
            radarsum1daydelay = str(radarSum48 - radarSum24)
          #2 day delay
          radarsum2daydelay = 'NULL'
          if(radarSum72 != 'NULL' and radarSum48 != 'NULL'):
            radarsum2daydelay = str(radarSum72 - radarSum48)
          #3 day delay
          radarsum3daydelay = 'NULL'
          if(radarSum96 != 'NULL' and radarSum72 != 'NULL'):
            radarsum3daydelay = str(radarSum96 - radarSum72)
           
          radarSum24 = str(radarSum24)      
          radarSum48 = str(radarSum48)      
          radarSum72 = str(radarSum72)      
          radarSum96 = str(radarSum96)      
          radarSum120 = str(radarSum120)      
          radarSum144 = str(radarSum144)      
          radarSum168 = str(radarSum168)      
                
                
          avgWindSpdSUN2 = self.getAvgWindSpeed('carocoops.SUN2.buoy', dateTime)
          if(avgWindSpdSUN2 == -9999):
            avgWindSpdSUN2 = 'NULL'
          else:
            avgWindSpdSUN2 = "%.3f"%(avgWindSpdSUN2)
          avgWindDirSUN2,cardPtSUN2 = self.getAvgWindDirection('carocoops.SUN2.buoy', dateTime)
          if(avgWindDirSUN2 == -9999):
            avgWindDirSUN2 = 'NULL'
          else:
            avgWindDirSUN2 = "%.3f"%(avgWindDirSUN2)
          avgSalinitySUN2 = self.getAvgSalinity('carocoops.SUN2.buoy', dateTime)
          if(avgSalinitySUN2 == -9999):
            avgSalinitySUN2 = 'NULL'
          else:
            avgSalinitySUN2 = "%.3f"%(avgSalinitySUN2)
          avgWaterTempSUN2 = self.getAvgWaterTemp('carocoops.SUN2.buoy', dateTime)
          if(avgWaterTempSUN2 == -9999):
            avgWaterTempSUN2 = 'NULL'
          else: 
            avgWaterTempSUN2 = "%.3f"%(avgWaterTempSUN2)
          avgWindSpdNOS = self.getAvgWindSpeed('nos.8661070.WL', dateTime)
          if(avgWindSpdNOS == -9999):
            avgWindSpdNOS = 'NULL'
          else:
            avgWindSpdNOS = "%.3f"%(avgWindSpdNOS)
            
          avgWindDirNOS,cardPtNOS = self.getAvgWindDirection('nos.8661070.WL', dateTime)
          if(avgWindDirNOS == -9999):
            avgWindDirNOS = 'NULL'
          else:
            avgWindDirNOS = "%.3f"%(avgWindDirNOS)
          avgWaterTempNOS = self.getAvgWaterTemp('nos.8661070.WL', dateTime)
          if(avgWaterTempNOS == -9999):
            avgWaterTempNOS = 'NULL'
          else:
            avgWaterTempNOS = "%.3f"%(avgWaterTempNOS)
          avgWaterLevelNOS = self.getAvgWaterLevel('nos.8661070.WL', dateTime)
          if(avgWaterLevelNOS == -9999):
            avgWaterLevelNOS = 'NULL'
          else:
            avgWaterLevelNOS = "%.3f"%(avgWaterLevelNOS)
          
          sql = "INSERT INTO station_summary \
                (date,station,rain_gauge,etcoc,salinity,tide,moon_phase,weather, \
                rain_summary_24,rain_summary_48,rain_summary_72,rain_summary_96,rain_summary_120,rain_summary_144,rain_summary_168, \
                rain_total_one_day_delay,rain_total_two_day_delay,rain_total_three_day_delay,\
                preceding_dry_day_count,inspection_type,rainfall_intensity_24,\
                radar_rain_summary_24,radar_rain_summary_48,radar_rain_summary_72,radar_rain_summary_96,radar_rain_summary_120,radar_rain_summary_144,\
                radar_rain_summary_168,radar_rain_total_one_day_delay,radar_rain_total_two_day_delay,radar_rain_total_three_day_delay,radar_preceding_dry_day_cnt,\
                radar_rainfall_intensity_24,sun2_wind_speed,sun2_wind_dir,sun2_water_temp,sun2_salinity,nos8661070_wind_spd,nos8661070_wind_dir,\
                nos8661070_water_temp,nos8661070_water_level ) \
                VALUES('%s','%s','%s',%s,%s,%s,%s,%s, \
                        %s,%s,%s,%s,%s,%s,%s, \
                        %s,%s,%s,\
                        %s,'%s',%s,\
                        %s,%s,%s,%s,%s,%s,%s,\
                        %s,%s,%s,%s,%s,\
                        %s,'%s',%s,%s,\
                        %s,'%s',%s,%s )" % \
                (estDatetime, station, rainGauge, etcoc, salinity, tide, moon, weather,
                  sum24, sum48, sum72, sum96, sum120, sum144, sum168,
                  sum1daydelay, sum2daydelay, sum3daydelay,
                  dryCnt, beachData['insp_type'], rainfallIntensity,
                  radarSum24,radarSum48,radarSum72,radarSum96,radarSum120,radarSum144,radarSum168,
                  radarsum1daydelay,radarsum2daydelay,radarsum2daydelay,radarDryCnt,radarIntensity,
                  avgWindSpdSUN2,cardPtSUN2,avgWaterTempSUN2,avgSalinitySUN2,
                  avgWindSpdNOS,cardPtNOS,avgWaterTempNOS,avgWaterLevelNOS)
          dbCursor = self.executeQuery(sql)
          if(dbCursor != None):
            if(commit):
              self.commit()
            if(self.logger != None):
              self.logger.info("Adding summary for station: %s date: %s." % (station, estDatetime))
            else:
              print("Adding summary for station: %s date: %s." % (station, estDatetime))
            dbCursor.close()
          else:
            self.logger.error(self.getErrorInfo())
        return(True)
      else:
        if(self.logger != None):
          self.logger.info("No data for station: %s date: %s. SQL: %s" % (station, estDatetime, sql))
        else:
          print("No data for station: %s date: %s. SQL: %s" % (station, estDatetime, sql))
        
    except Exception, e:
      if(self.logger != None):
        self.logger.critical(str(e) + ' Terminating script.', exc_info=1)
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
      sys.exit(-1)
    
    return(False)
  
  def createXMRGStats(self, dateTime, platformHandle):
    xmrgData = {}
    #Query the rainfall totals over the given hours range. 
    #Get the last 24 hours summary
    xmrgData['radarSum24'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 24)
    if(xmrgData['radarSum24'] == -9999.0):
      xmrgData['radarSum24'] = None
    #Get the last 48 hours summary
    xmrgData['radarSum48'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 48)
    if(xmrgData['radarSum48'] == -9999.0):
      xmrgData['radarSum48'] = None
    #Get the last 72 hours summary
    xmrgData['radarSum72'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 72)
    if(xmrgData['radarSum72'] == -9999.0):
      xmrgData['radarSum72'] = None
    #Get the last 96 hours summary
    xmrgData['radarSum96'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 96)
    if(xmrgData['radarSum96'] == -9999.0):
      xmrgData['radarSum96'] = None
    #Get the last 120 hours summary
    xmrgData['radarSum120'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 120)
    if(xmrgData['radarSum120'] == -9999.0):
      xmrgData['radarSum120'] = None
    #Get the last 144 hours summary
    xmrgData['radarSum144'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 144)
    if(xmrgData['radarSum144'] == -9999.0):
      xmrgData['radarSum144'] = None
    #Get the last 168 hours summary
    xmrgData['radarSum168'] = self.getLastNHoursSummaryFromRadarPrecip(dateTime, platformHandle, 168)
    if(xmrgData['radarSum168'] == -9999.0):
      xmrgData['radarSum168'] = None
  
    xmrgData['radarIntensity'] = self.calcRadarRainfallIntensity( platformHandle, dateTime, 60)
    if(xmrgData['radarSum24'] == 'NULL' or xmrgData['radarIntensity'] == -9999):
      xmrgData['radarIntensity'] = None
    
    xmrgData['radarDryCnt'] = None
    if(xmrgData['radarSum24'] != None):
      xmrgData['radarDryCnt'] = self.getPrecedingRadarDryDaysCount(dateTime, platformHandle)
      if(xmrgData['radarDryCnt'] == -9999):
        xmrgData['radarDryCnt'] = None
    
    #calculate the X day delay totals
    #1 day delay
    xmrgData['radarsum1daydelay'] = None
    if(xmrgData['radarSum48'] != None and xmrgData['radarSum24'] != None):
      xmrgData['radarsum1daydelay'] = (xmrgData['radarSum48'] - xmrgData['radarSum24'])
    #2 day delay
    xmrgData['radarsum2daydelay'] = None
    if(xmrgData['radarSum72'] != None and xmrgData['radarSum48'] != None):
      xmrgData['radarsum2daydelay'] = (xmrgData['radarSum72'] - xmrgData['radarSum48'])
    #3 day delay
    xmrgData['radarsum3daydelay'] = None
    if(xmrgData['radarSum96'] != None and xmrgData['radarSum72'] != None):
      xmrgData['radarsum3daydelay'] = (xmrgData['radarSum96'] - xmrgData['radarSum72'])    
    
    return(xmrgData)
     
  def getLastNHoursPrecipSummary(self, dateTime, mTypeID, platformHandle, prevHourCnt):
    sql = "SELECT SUM(m_value) \
           FROM multi_obs \
           WHERE\
             m_date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s' ) AND\
             m_type_id = %d AND\
             platform_handle = '%s'"\
             % (dateTime, prevHourCnt, dateTime,mTypeID,platformHandle)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      sum = dbCursor.fetchone()[0]
      if(sum != None):
        return(float(sum))      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    return(-9999.0)
  
  """
  Function: getLastNHoursSummaryFromRadarPrecipSummary
  Purpose: Calculate the rainfall summary for the past N hours for a given rain_gauge.
  Parameters:
    dateTime is the date and time we want to start our collection
    rain_gauge is the rain gauge we are query.  
    prevHourCnt is the number of hours to go back from the given dateTime above.
  """
  def getLastNHoursSummaryFromPrecipSummary(self, dateTime, rain_gauge, prevHourCnt):
    platformHandle = 'dhec.%s.raingauge' % (rain_gauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation_accumulated_daily', 'in',platformHandle,1)
    sum = self.getLastNHoursPrecipSummary(dateTime, mTypeID, platformHandle, prevHourCnt)
    return(sum)
  """
  Function: getLastNHoursSummaryFromRadarPrecipSummary
  Purpose: Calculate the rainfall summary for the past N hours for a given rain_gauge/radar.
  Parameters:
    dateTime is the date and time we want to start our collection
    rain_gauge is the rain gauge we are query. Even though we are looking up radar data, the 
      rain_gauge name is used to denote the radar area of interest.
    prevHourCnt is the number of hours to go back from the given dateTime above.
  """
  def getLastNHoursSummaryFromRadarPrecip(self, dateTime, rain_gauge, prevHourCnt):
    sum = None
    platformHandle = 'nws.%s.radar' % (rain_gauge)
    #mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation_radar_weighted_average', 'in',platformHandle,1)
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    sensorID = xeniaSQLite.sensorExists(self, 'precipitation_radar_weighted_average', 'in', platformHandle)
    
    #if(mTypeID != None):
      #sum = self.getLastNHoursPrecipSummary(datetime, mTypeID, platformHandle, prevHourCnt)
      
    if(sensorID != None and sensorID != -1):
      """
      sql = "SELECT SUM(m_value) \
             FROM multi_obs \
             WHERE\
               m_date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
               m_date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s' ) AND\
               sensor_id = %d"\
               % (datetime, prevHourCnt, datetime, sensorID)
      """
      sql = "SELECT SUM(m_value) \
             FROM multi_obs \
             WHERE\
               m_date >= strftime('%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' )) AND \
               m_date < '%s' AND\
               sensor_id = %d AND m_value > 0.0;"\
               % (dateTime, prevHourCnt, dateTime, sensorID)
      try:
        dbCursor = self.executeQuery(sql)
        if(dbCursor != None):
          sum = dbCursor.fetchone()[0]
          if(sum != None):
            sum = float(sum)
          #Currently we are not putting radar summaries of 0 in the database, so if our query doesn't return
          #any records, then our sum is 0.
          else:
            sum = 0.0
      except sqlite3.Error, e:
        self.rowErrorCnt += 1
        if(self.logger != None):
          self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        else:
          print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    else:
      if(self.logger != None):
        self.logger.error("No sensor id found for platform: %s." % (platformHandle))
    
    return(sum)  

  def getLastNHoursSummary(self, dateTime, rain_gauge, prevHourCnt):
    platformHandle = 'dhec.%s.raingauge' % (rain_gauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation', 'in',platformHandle,1)
    sql = "SELECT SUM(m_value) \
           FROM multi_obs \
           WHERE\
             m_date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s') ) AND\
             m_type_id = %d AND\
             platform_handle = '%s'"\
             % (dateTime, prevHourCnt, dateTime,mTypeID,platformHandle)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      sum = dbCursor.fetchone()[0]
      if(sum != None):
        return(float(sum))      
    
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))      
    return(-9999.0)
  """
  Function: getPrecedingDryDaysCount
  Purpose: For the given date, this function calculates how many days previous had no rainfall, if any.
  Parameters: 
    dateTime is the dateTime we are looking for
    rainGauge is the rain  gauge we are query the rainfall summary for.
  """
  def getPrecedingDryDaysCount(self, dateTime, rainGauge):
    iDryCnt = 0
    secondsInDay = 24 * 60 * 60
    platformHandle = "dhec.%s.raingauge" %(rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self,"precipitation_accumulated_daily","in",platformHandle,1)
    #When determining the number of days back to go, we use 2 in the  m_date <=strftime('%%Y-%%m-%%dT23:59:00', datetime('%s', '-2 day') )
    #comparison below since the dates on the rain gauge summary data is at hour 23:59.
    #Might change this to drop the hour/minute/second format which would then allow -1 day to be
    #used and be more sensible. Need to test first.
    sql = "Select A.m_date, A.row_id,  A.m_value, "\
          "Case "\
          "When A.m_value= 0  Then "\
              "IFNULL( (Select Max(B.row_id) From multi_obs As B WHERE B.row_id < A.row_id AND B.m_value=0   ), "\
                       "(A.row_id) ) "\
                         "End As grp "\
          "FROM multi_obs As A WHERE "\
          "m_date <= strftime('%%Y-%%m-%%dT23:59:00', datetime('%s', '-2 day') ) AND "\
          "m_type_id = %d AND "\
          "platform_handle = '%s' "\
          "ORDER BY datetime(m_date) DESC;"\
          %(dateTime,mTypeID,platformHandle)          
    dbCursor = xeniaSQLite.executeQuery(self,sql)
    if(dbCursor != None):    
      #We subtract off a day since when we are looking for the summaries we start on -1 day from the date provided.
      t1 = time.mktime(time.strptime(dateTime, '%Y-%m-%dT%H:%M:00')) - secondsInDay
      for row in dbCursor:
        if(row['grp'] != None):
          t2 = time.mktime(time.strptime(row['m_date'], '%Y-%m-%dT%H:%M:00'))         
          #We have to make sure the data points are 1 day apart, according the thesis if we are missing
          #a data point, we don't calculate the preceding dry days.
          if((t1 - t2) <= secondsInDay):
            iDryCnt += 1
            t1 = t2
          else:
            iDryCnt = -9999
            break
        else:
          break
    else:
      iDryCnt = -9999
    return(iDryCnt)
  
  """
  Function: getPrecedingRadarDryDaysCount
  Purpose: For the given date, this function calculates how many days previous had no rainfall, if any.
  Radar data differs from the rain gauge data in that when there is no rainfall recorded, we will not have
  a record showing 0 rain fall. This function exploits this fact by querying the database and limiting the
  return data to 1 record which will be the last date that we had rainfall.
  Parameters: 
    dateTime is the dateTime we are looking for
    rainGauge is the rain  gauge we are query the rainfall summary for.
  """
  def getPrecedingRadarDryDaysCount(self, dateTime, rainGauge):
    
    iDryCnt = -9999
    platformHandle = "nws.%s.radar" %(rainGauge)
    sensorId = xeniaSQLite.sensorExists(self,"precipitation_radar_weighted_average", "in", platformHandle)
    if(sensorId != None and sensorId != -1):
      #We want to start our dry day search the day before our dateTime.
      sql = "SELECT m_date FROM multi_obs WHERE m_date < '%s' AND sensor_id=%d AND m_value > 0 ORDER BY m_date DESC LIMIT 1;"\
            %(dateTime,sensorId)          
      dbCursor = xeniaSQLite.executeQuery(self,sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row != None):
          firstVal = datetime.datetime.strptime(row['m_date'], "%Y-%m-%dT%H:%M:%S")
          startDate = datetime.datetime.strptime(dateTime,"%Y-%m-%dT%H:%M:%S")
          delta = startDate - firstVal
          iDryCnt = delta.days
      else:
        if(self.logger != None):
          self.logger.error("getPrecedingRadarDryDaysCount failed to retrieve data for platform: %s. %s"\
                            % (platformHandle,self.lastErrorMsg))
        iDryCnt = None
    else:
      if(self.logger != None):
        self.logger.error("No sensor id found for platform: %s." % (platformHandle))
      iDryCnt = None
    return(iDryCnt)
    """        
    secondsInDay = 24 * 60 * 60
    platformHandle = "nws.%s.radar" %(rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self,"precipitation_radar_weighted_average","in",platformHandle,1)
    sql = "SELECT DISTINCT(strftime('%%Y-%%m-%%d', m_date)) AS m_date\
          FROM multi_obs WHERE \
          m_date <= strftime('%%Y-%%m-%%d', datetime('%s', '-1 day') ) AND \
          m_type_id = %d AND \
          platform_handle = '%s' \
          ORDER BY datetime(m_date) DESC LIMIT 1;"\
          %(dateTime,mTypeID,platformHandle)          
    dbCursor = xeniaSQLite.executeQuery(self,sql)
    if(dbCursor != None):    
      row = dbCursor.fetchone()
      if(row != None):
        #Now let's figure out the dry days.
        #Convert the dateTime into a struct_time.
        startDate = time.strptime(dateTime, "%Y-%m-%dT%H:%M:%S")
        #Now we get rid of the hour/min/sec portion and just deal with the day/
        startDate = time.strptime(time.strftime('%Y-%m-%dT00:00:00', startDate), '%Y-%m-%dT00:00:00')
        #Convert to an epoch time.
        startDate = time.mktime(startDate)
        #Convert to an epoch time using the year month and day.
        endDate = time.mktime(time.strptime(row['m_date'], "%Y-%m-%d"))
        #Determine the day count.
        iDryCnt = (startDate - endDate) / secondsInDay
    else:
      iDryCnt = None
    """    
    return(iDryCnt)
      
  
  """  
  Function: calcRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 10 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    platformHandle is the name of the platform we are investigating.
    mTypeID is the id of the tpye of rain observation we are looking up. 
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    intervalInMinutes is the number of number of minutes each sample represents.
  """ 
  def calcIntensity(self, platformHandle, mTypeID, date, intervalInMinutes):
    rainfallIntensity = -9999.0
    
    #Get the entries where there was rainfall for the date, going forward the minutes number of minutes.
    """ 
    sql = "SELECT m_value from multi_obs \
            WHERE \
            m_date >= strftime('%%Y-%%m-%%dT00:00:00', datetime('%s','-1 day') ) AND m_date < strftime('%%Y-%%m-%%dT00:00:00', '%s' ) AND\
            m_type_id = %d AND\
            platform_handle = '%s';"\
            % (date, date, mTypeID, platformHandle)
    """
    sql = "SELECT m_value from multi_obs \
            WHERE \
            m_date >= strftime('%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s','-1 day') ) AND m_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s' ) AND\
            m_type_id = %d AND\
            platform_handle = '%s';"\
            % (date, date, mTypeID, platformHandle)
    dbCursor =  xeniaSQLite.executeQuery(self,sql)
    if(dbCursor != None):
      totalRainfall = 0
      numRainEntries = 0    
      hasData = False  
      for row in dbCursor:
        rainfall = float(row['m_value'])
        hasData = True
        if(rainfall > 0.0):
          totalRainfall += rainfall
          numRainEntries += 1
      
      #We want to check to make sure we have data from our query, if we do, let's zero out rainfallIntesity.
      #Otherwise we want to leave it at -1 to denote we had no data for that time.
      if(hasData == True):
        rainfallIntensity = 0.0
          
      if(numRainEntries):  
        rainfallIntensity = totalRainfall / (numRainEntries * intervalInMinutes)
    else:    
      self.logger.error("%s Function: %s Line: %s File: %s" % (self.lastErrorMsg,self.lastErrorFunc, self.lastErrorLineNo, self.LastErrorFile))
  
    return(rainfallIntensity)
  
  """  
  Function: calcRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 10 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    rainGauge is the name of the rain gauge we are investigating.
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    intervalInMinutes is the number of number of minutes each sample represents.

  """ 
  def calcRainfallIntensity(self, rainGauge, date, intervalInMinutes=10):
    rainfallIntensity = -9999.0
    platformHandle = 'dhec.%s.raingauge' % (rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation', 'in',platformHandle,1)
    if(mTypeID != None):
      rainfallIntensity = self.calcIntensity( platformHandle, mTypeID, date, intervalInMinutes)
    else:
      self.logger.error("No m_type_id for: precipitation(in) found for platform: %s" %(platformHandle))
                        
    return(rainfallIntensity)
    
  """  
  Function: calcRadarRainfallIntensity
  Purpose: 2.  Rainfall Intensity- calculated on a per day basis as the total rain per day in inches divided 
  by the total amount of time in minutes over which that rain fell.  
  This was calculated by dividing the 24-hour total rainfall by the number of 60 minute increments in the day 
  with recorded rain multiplied by ten----[Total Rain/(# increments with rain *10)]
  Parameters:
    rainGauge is the name of the rain gauge we are investigating.
    date is the end date/time of where we want to start getting the rainfall data. We search back 1 day from this date.
    intervalInMinutes is the number of number of minutes each sample represents.
  """ 
  def calcRadarRainfallIntensity(self, rainGauge, date, intervalInMinutes=60):
    rainfallIntensity = -9999.0
    platformHandle = 'nws.%s.radar' % (rainGauge)
    mTypeID = xeniaSQLite.getMTypeFromObsName(self, 'precipitation_radar_weighted_average', 'in',platformHandle,1)
    if(mTypeID != None):
      rainfallIntensity = self.calcIntensity( platformHandle, mTypeID, date, intervalInMinutes)
      #We do not store radar data that has 0 for precipitation, so we want to make sure not to send
      #-1.0 as the return value as -1.0 indicates no data due to a data problem.
      if(rainfallIntensity == -9999.0):
        rainfallIntensity = 0.0              
    else:
      self.logger.error("No m_type_id for: precipitation_radar_weighted_average(in) found for platform: %s" %(platformHandle))
    return(rainfallIntensity)

  """
  Function: getTideLevel
  Purpose: For the given tide station and date, this retrieves the tidal information.
  Tide data is MLLW at local time, daily Hi/Lo tide points.
  Parameters: 
    tideStationID is the station to retrieve the data from.
    date is the date to retrieve.
  """    
  def getTideLevel(self, tideStationID, date):
    tideLevel = -9999
    #0 is Full stage, either Ebb or Flood, 100 is 1/4, 200 is 1/2 and 300 is 3/4. Below we add either
    #the 2000 for flood or 4000 for ebb.
    tideStages = [0, 100, 200, 300]
    sql = "SELECT date,level,level_code FROM daily_tide_range \
            WHERE station_id = %d AND ( date >= strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s', '-12 hours') AND \
            date < strftime( '%%Y-%%m-%%dT%%H:%%M:%%S', '%s', '12 hours') ) \
            ORDER BY date ASC" % (tideStationID, date, date) 
    try:
      epochT1 = time.mktime(time.strptime(date, '%Y-%m-%dT%H:%M:%S'))
      prevTime = -1.0
      curTime = -1.0
      prevRow = []
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      for row in dbCursor:
        curTime = time.mktime(time.strptime(row['date'], '%Y-%m-%dT%H:%M:%S'))
        if(prevTime != -1.0):
          if((epochT1 >= prevTime) and 
              (epochT1 < curTime)):
              #Figure out if the tide is in Ebb or Flood
              tideState = -1
              prevLevel = float(prevRow['level'])
              curLevel = float(row['level'])
              if(prevLevel < curLevel):
                tideState = 2000
              else:
                tideState = 4000
              #Now figure out if it is 0, 1/4, 1/2, 3/4 stage. We divide the time between the 2 tide changes
              #up into 4 pieces, then figure out where our query time falls.
              totalTime = curTime - prevTime
              qtrTime = totalTime / 4.0
              tidePos = -1
              
              for i in range(0, 4):
                if((epochT1 >= prevTime) and
                    (epochT1 < (prevTime + qtrTime))):
                   tidePost = i
                   tideLevel = tideState + tideStages[i]
                   return(tideLevel)
                   
                prevTime += qtrTime
        prevRow = row
        prevTime = curTime
                
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
    return(tideLevel)
  """
  Function: getPlatforms
  Purpose: Returns the platforms(rain gauges, monitoring stations) from the platforms table
  """
  def getPlatforms(self,where=""):
    try:
      sql = "SELECT * from platform %s;" %(where) 
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      return(dbCursor)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):      
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        
    return(None)
  """
  Function: getRainGauges
  Purpose: Returns the rain gauges from the platforms table
  """
  def getRainGauges(self, active=None):
    try:
      whereClause = '';
      if(active != None):
        whereClause = " AND active=%d" % (active)
      sql = "SELECT * from platform WHERE platform_handle LIKE '%%raingauge%%' %s" % (whereClause)
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
      return(dbCursor)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
    return(None)
  """
  Function: getRainGaugeForStation
  Purpose: For the given station name, returns the associated rain gauge.
  Parameters: stationName is the string representing the station. 
  Returns: If none found, then an empty string is returned, if an error occured
    None is returned.
  """
  def getRainGaugeForStation(self, stationName):
    try:
      rainGauge = ''
      sql = "SELECT rain_gauge FROM monitoring_stations WHERE station='%s';"\
            %(stationName)
      dbCursor = self.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row != None):
          rainGauge = row['rain_gauge']
        dbCursor.close()
      return(rainGauge)
    except sqlite3.Error, e:
      self.rowErrorCnt += 1
      if(self.logger != None):
        self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
      else:
        print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
    return(None)
  """
  Function: getLastXMRGDate
  Purpose: Returns the last date collected for radar data.
  Parameters:
  Return: String with the last date, or None if nothing found.
  """
  def getLastXMRGDate(self):
    try:
      sql = "SELECT max(collection_date) AS date FROM precipitation_radar"
      dbCursor = self.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row != None):
          return(row['date'])
    except Exception, E:
      self.logger.error(str(E) + ' Terminating script')
      sys.exit(- 1)
    return(None)
 
  """
  Function: cleanPrecipRadar
  Purpose: This function will remove all data older the olderThanDate from the precipitation_radar table.
  Parameters:
    olderThanDate is the comparison date to use.
  Return: 
    True if successful, otherwise False.
  """
  def cleanPrecipRadar(self, olderThanDate):
    sql = "DELETE FROM precipitation_radar WHERE collection_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s');" % (olderThanDate)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      try:
        self.DB.commit()
        return(True)  
      except sqlite3.Error, e:
        self.rowErrorCnt += 1
        if(self.logger != None):
          self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        else:
          print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        
    return(False)  
  """
  Function: cleanIOOSData
  Purpose: Deletes IOOS data from the database that is older than the number of days we want to keep, set in the
    parameter NDaysToKeep.
  Parameters:
    NDaysToKeep is the number of days we want to remain in the database.
  """
  def cleanIOOSData(self, NDaysToKeep):
    sql = "DELETE FROM multi_obs WHERE m_date < datetime('now', '-%d days') AND platform_handle NOT LIKE '%dhec%';"
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      try:
        self.DB.commit()
        return(True)  
      except sqlite3.Error, e:
        if(self.logger != None):
          self.logger.error("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        else:
          print("ErrMsg: %s SQL: '%s'" % (e.args[0], sql))
        return(False)
    return(None)
  """
  Function: getRadarDataForBoundary
  Purpose: For the given rain gauge(boundaryName), this function will return the radar data that is in that POLYGON.
  """
  def getRadarDataForBoundary(self, boundaryName,strtTime,endTime):
    sql = "SELECT latitude,longitude,precipitation FROM precipitation_radar \
            WHERE\
            collection_date >= '%s' AND collection_date < '%s'\
            Intersects( Geom, \
                        (SELECT Geometry FROM boundaries WHERE AOI ='%s'))"\
            %(strtTime,endTime,boundaryName)
    return(self.executeQuery(self,sql))
  
  """
  Function: calculateWeightedAvg
  Purpose: For a given station(rain gauge) this function queries the radar data, gets the grids that fall
   into the watershed of interest and calculates the weighted average.
  Parameters:
    watershedName is the watershed we want to calculate the average for. For ease of use, I use the rain gauge name to 
       name the watersheds.
    startTime is the starting time in YYYY-MM-DDTHH:MM:SS format.
    endTime is the starting time in YYYY-MM-DDTHH:MM:SS format.
  """
  def calculateWeightedAvg(self, watershedName, startTime, endTime):
    weighted_avg = -9999
    #Get the percentages that the intersecting radar grid make up of the watershed boundary.      
    sql = "SELECT * FROM(\
           SELECT (Area(Intersection(radar.geom,bounds.Geometry))/Area(bounds.Geometry)) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar, boundaries bounds\
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                bounds.AOI = '%s' AND\
                Intersects(radar.geom, bounds.geometry))"\
                %(startTime,endTime,watershedName)
    dbCursor = self.executeQuery(sql)        
    if(dbCursor != None):
      total = 0.0
      date = ''
      cnt = 0
      for row in dbCursor:
        percent = row['percent']
        precip = row['precipitation']
        total += (percent * precip)
        cnt += 1
      dbCursor.close()
      if(cnt > 0):
        weighted_avg = total
    else:
      weighted_avg = None
    return(weighted_avg)
  
  """
  Function: importTideFile
  Purpose: Imports the tide file passed in the tideFilePath string.
  Parameters:
    tideFilePath is the fully qualified path the the file to import.
  """
  def importTideFile(self,tideFilePath):
    from dhecRainGaugeProcessing import processTideData
    
    tideFile = processTideData()
    tideFile.openFile(tideFilePath)
    stationID, date, level, tideType = tideFile.readLine()
    
    while(stationID != None and stationID != ""):      
      if(stationID != None):
        #print( "ID: %s Date: %s Level: %f Type: %s" %(stationID, date, level, tideType))
        stationID, date, level, tideType = tideFile.readLine()
        sql = "INSERT INTO daily_tide_range (station_id,date,level,level_code)\
                VALUES(%d,'%s',%f,'%s')"\
                %(int(stationID),date,level,tideType)
        dbCursor = self.executeQuery(sql)
        if(dbCursor == None):
          #We had an error, so we're going to bail.
          return(None)
        
      else:
        if(self.logger != None):
          self.error("Error importing tide file: %s on line: %d" %(tideFilePath,tideFile.lineNo))
        return(False)
      
      stationID, date, level, tideType = tideFile.readLine()
    #Commit the entries into the database.
    self.commit()
    return(True)  
  """
  Function: getAverageForObs\
  Purpose: For the given observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    obsName string representing the observation we are calcing the average for
    uom the units of measurement the observation is stored in
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAverageForObs(self, obsName, uom, platformHandle, startDate, prevNHours):
    avg = -9999
    
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    sensorID = self.sensorExists(obsName, uom, platformHandle)
    if(sensorID != None and sensorID != -1):
      sql = "SELECT AVG(m_value) as m_value_avg  FROM multi_obs\
             WHERE sensor_id = %d AND\
             m_date >= strftime( '%%Y-%%m-%%dT00:00:00', datetime( '%s', '-%d hours' ) ) AND \
             m_date < strftime( '%%Y-%%m-%%d00:00:00', '%s' )"\
            %(sensorID, startDate, prevNHours, startDate) 
      dbCursor = self.executeQuery(sql)
      if(dbCursor != None):
        row = dbCursor.fetchone()
        if(row['m_value_avg'] != None):
          avg = float(row['m_value_avg'])
        return(avg)
    return(None)
  
  """
  Function: getAvgWindSpeed
  Purpose: For the wind_speed observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgWindSpeed(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('wind_speed', 'm_s-1', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    else:
      #Convert to knots.
      if(avg != -9999):
        avg *= 1.9438444924406
    return(avg)

  """
  Function: getAvgWindDirection
  Purpose: For the wind_from_direction observation on the platform, will compute the data average from the starting date
  back N hours. Returns the Cardinal Compass Point.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  Return:
    The avg direction as well as the cardinal point
  """
  def getAvgWindDirection(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('wind_from_direction', 'degrees_true', platformHandle, startDate, prevNHours)
    cardinalPt = ""
    if(avg == None):
      avg = -9999
    else:
      cardinalPt = self.compassDirToCardinalPt(avg)
    return(avg,cardinalPt)

  """
  Function: getAvgWaterTemp
  Purpose: For the water_temperature observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgWaterTemp(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('water_temperature', 'celsius', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    return(avg)
 
  """
  Function: getAvgSalinity
  Purpose: For the salinity observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgSalinity(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('salinity', 'psu', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    return(avg)

  """
  Function: getAvgWaterLevel
  Purpose: For the water_level observation on the platform, will compute the data average from the starting date
  back N hours.
  Parameters:
    platformHandle the platform the observation was made on
    startDate the date to start the average
    prevNHours the number of hours to go back from the startDate to compute the average.
  """
  def getAvgWaterLevel(self, platformHandle, startDate, prevNHours=24):
    avg = self.getAverageForObs('water_level', 'm', platformHandle, startDate, prevNHours)
    if(avg == None):
      avg = -9999
    return(avg)
    
  """
  Function: compassDirToCardinalPt
  Purpose: Given a 0-360 compass direction, this function will return the cardinal point for it.
    Compass is broken into 8 points: N, NE, E, SE, S, SW, W, NW
  Parameters:
    compassDir is the compass direction to compute the cardinal point for.
  """
  def compassDirToCardinalPt(self, compassDir):
    if(compassDir >= 0 and compassDir <= 360):
      #Get the cardinal point. We use an 8 point system, N, NE, E, SE, S, SW, W, NW
      #We have to "wrap" N since out valid compassDir values are 0-360. 
      cardinalPts = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "N"]
      degreeOffset = 360 / 8
      cardPt = int(round(compassDir / degreeOffset, 0))
      if(cardPt < 9):
        return(cardinalPts[cardPt])
    return(None)              

  """
  Function: backupData
  Purpose: Rollover precipitation data in the live database older than 30 days into the backup database.
  """    
  def backupData(self, dbBackupFile, dbBackupSQLSchemaFile):
    self.logger.info("Beginning data backup/rollover.")
    
    curYear = time.strftime('%Y', time.localtime())
    backupDB = None
    filePath = "%s%s/" % (dbBackupFile, curYear)
    backupFilename = "%s%s-dhec.db" % (filePath, curYear)
    ################################################################################
    #Test to see if the database exists.
    if(not os.path.isfile(backupFilename)):    
      #Check to see if the directory exists
      if(not os.path.exists(filePath)):
        os.mkdir(filePath)
        self.logger.info("Directory: %s does not exist, creating." % (filePath))
      
      if(dbBackupSQLSchemaFile == None):  
        self.logger.info("File: %s does not exist, cannot continue." % (backupFilename))
        return
      #We've got a SQL file to create the schema with.
      else:
        backupDB = dhecDB(backupFilename, "dhec_logger")
        backupDB.DB.close()
        shellCmd = "sqlite3 \"%s\" < \"%s\""%(backupFilename,dbBackupSQLSchemaFile)
        ret = os.system(shellCmd)
        self.logger.debug("Created database: %s with schema file: %s" %(backupFilename,dbBackupSQLSchemaFile))
    else:
      backupDB = dhecDB(backupFilename, "dhec_logger")
      self.logger.info("Connecting to database: %s" % (backupFilename))

    ################################################################################
      
    ################################################################################
    #On a platform by platform basis, get all data that is not in the current month.    
    dbCursor = self.getPlatforms()
    #Cutoff date, we want to keep last 30 days.      
    cutoffDate = time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time() - (30 * 24 * 60 * 60)))
    #for platformHandle in platList:
    for row in dbCursor: 
      platformHandle = row['platform_handle']   
      if(self.logger):
        self.logger.info("Processing multi_obs table data for platform: %s" % (platformHandle))
      sql = "SELECT m_date,m_lat, m_lon,m_z\
                ,multi_obs.platform_handle \
                ,obs_type.standard_name \
                ,uom_type.standard_name as uom \
                ,multi_obs.m_type_id \
                ,m_value \
                ,qc_level \
                ,sensor.row_id as sensor_id\
                ,sensor.s_order \
              FROM multi_obs \
                left join sensor on sensor.row_id=multi_obs.sensor_id \
                left join m_type on m_type.row_id=multi_obs.m_type_id \
                left join m_scalar_type on m_scalar_type.row_id=m_type.m_scalar_type_id \
                left join obs_type on obs_type.row_id=m_scalar_type.obs_type_id \
                left join uom_type on uom_type.row_id=m_scalar_type.uom_type_id \
                WHERE m_date < '%s' AND\
                multi_obs.platform_handle = '%s'\
                ORDER BY m_date DESC" \
                % (cutoffDate,platformHandle)        
      resultsCursor = self.executeQuery(sql)
      rowCnt = 0
      if(resultsCursor != None):
        for item in resultsCursor:
          obsName = ''
          uom = ''
          mVals = []
          mVals.append(float(item['m_value']))
          z = item['m_z']
          if(z == None):
            z = 0
          sOrder = item['s_order']
          if(sOrder == None):
            sOrder = 1
          if(backupDB.addMeasurement(item['standard_name'], 
                                     item['uom'],
                                     platformHandle,
                                     item['m_date'],
                                     item['m_lat'], item['m_lon'],
                                     z,
                                     mVals,
                                     sOrder,
                                     False) != True):
            if(self.logger):
              self.logger.critical( "%s Function: %s Line: %d File: %s"\
                                 %(backupDB.lastErrorMsg,backupDB.lastErrorFunc, backupDB.lastErrorLineNo, backupDB.lastErrorFile) )
            
          
          rowCnt += 1               
        if(not backupDB.commit()):
          self.logger.error(backupDB.lastErrorMsg)
          sys.exit(- 1)
        self.logger.info("Successfully processed and committed: %d rows into backup." % (rowCnt))
        resultsCursor.close()
        
        #Now we delete the records from the source DB.
        self.logger.info("Deleting backed up records from source database.")
        sql = "DELETE FROM multi_obs WHERE m_date < '%s' and platform_handle='%s'" % (cutoffDate, platformHandle)
        resultsCursor = self.executeQuery(sql)
        if(resultsCursor != None):
          if(not self.commit()):
            self.logger.error(self.lastErrorMsg)
            sys.exit(- 1)
        else:
          self.logger.error(self.lastErrorMsg)
          sys.exit(- 1)
        resultsCursor.close()
    dbCursor.close()
   
    self.logger.info("Finished data backup/rollover.")

  