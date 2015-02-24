import sys
sys.path.append('../commonfiles')

from pysqlite2 import dbapi2 as sqlite3
from dhecDB import dhecDB
from datetime import datetime, timedelta
from stats import vectorMagDir

class wqDB(dhecDB):
  """
  Function: getLastNHoursSummaryFromRadarPrecipSummary
  Purpose: Calculate the rainfall summary for the past N hours for a given rain_gauge/radar.
  Parameters:
    dateTime is the date and time we want to start our collection
    rain_gauge is the rain gauge we are query. Even though we are looking up radar data, the
      rain_gauge name is used to denote the radar area of interest.
    prevHourCnt is the number of hours to go back from the given dateTime above.
  """
  def getLastNHoursSummaryFromRadarPrecip(self, platform_handle, dateTime, prevHourCnt, obs_type, uom):
    sum = None
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    sensorID = dhecDB.sensorExists(self, obs_type, uom, platform_handle)


    if(sensorID != None and sensorID != -1):
      start_date = dateTime - timedelta(hours=prevHourCnt)
      #m_date >= strftime('%%Y-%%m-%%dT%%H:%%M:%%S', datetime( '%s', '-%d hours' )) AND \

      sql = "SELECT SUM(m_value) \
             FROM multi_obs \
             WHERE\
               m_date >= '%s' AND\
               m_date < '%s' AND\
               sensor_id = %d AND m_value >= 0.0;"\
               % (start_date.strftime('%Y-%m-%dT%H:%M:%S'), dateTime.strftime('%Y-%m-%dT%H:%M:%S'), sensorID)
      try:
        dbCursor = self.DB.cursor()
        dbCursor.execute(sql)
      except sqlite3.Error, e:
        if self.logger:
          self.logger.exception(e)
      else:
        sum = dbCursor.fetchone()[0]
        if sum:
          sum = float(sum)
    else:
      if self.logger:
        self.logger.error("No sensor id found for platform: %s." % (platform_handle))

    return sum

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
  def getPrecedingRadarDryDaysCount(self, platform_handle, dateTime, obs_type, uom):

    dry_cnt = -9999
    sensorId = dhecDB.sensorExists(self, obs_type, uom, platform_handle)
    if sensorId != None and sensorId != -1:
      #We want to start our dry day search the day before our dateTime.
      sql = "SELECT m_date FROM multi_obs WHERE m_date < '%s' AND sensor_id=%d AND m_value > 0 ORDER BY m_date DESC LIMIT 1;"\
            % (dateTime.strftime("%Y-%m-%dT%H:%M:%S"), sensorId)
      try:
        dbCursor = self.DB.cursor()
        dbCursor.execute(sql)
      except sqlite3.Error, e:
        if self.logger:
          self.logger.exception(e)
        dry_cnt = None
      else:
          row = dbCursor.fetchone()
          if row:
            first_val = datetime.strptime(row['m_date'], "%Y-%m-%dT%H:%M:%S")
            #Get rid of the time zone.
            start_date = datetime.strptime(dateTime.strftime("%Y-%m-%dT%H:%M:%S"), "%Y-%m-%dT%H:%M:%S")
            delta = start_date - first_val
            dry_cnt = delta.days
    else:
      if self.logger:
        self.logger.error("No sensor id found for platform: %s." % (platform_handle))
      dry_cnt = None
    return dry_cnt


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
  def calcIntensity(self, platform_handle, sensor_id, dateTime, intervalInMinutes):
    rainfallIntensity = -9999.0

    #Get the entries where there was rainfall for the date, going forward the minutes number of minutes.
    start_date = dateTime - timedelta(days=1)
    #m_date >= strftime('%%Y-%%m-%%dT%%H:%%M:%%S', datetime('%s','-1 day') )
    #AND m_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s' ) AND\
    sql = "SELECT m_value from multi_obs \
            WHERE \
            m_date >= '%s' AND m_date < '%s' AND\
            sensor_id = %d AND\
            platform_handle = '%s';"\
            % (start_date.strftime("%Y-%m-%dT%H:%M:%S"), dateTime.strftime("%Y-%m-%dT%H:%M:%S"), sensor_id, platform_handle)
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute(sql)
    except sqlite3.Error, e:
      if self.logger:
        self.logger.exception(e)
    else:
      totalRainfall = 0
      numRainEntries = 0
      hasData = False
      for row in dbCursor:
        rainfall = float(row['m_value'])
        hasData = True
        if rainfall > 0.0:
          totalRainfall += rainfall
          numRainEntries += 1

      #We want to check to make sure we have data from our query, if we do, let's zero out rainfallIntesity.
      #Otherwise we want to leave it at -1 to denote we had no data for that time.
      if hasData:
        rainfallIntensity = 0.0

      if numRainEntries:
        rainfallIntensity = totalRainfall / (numRainEntries * intervalInMinutes)

    return rainfallIntensity

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
  def calcRadarRainfallIntensity(self, platform_handle, date, intervalInMinutes=60, obs_type='precipitation_radar_weighted_average', uom='mm'):
    rainfallIntensity = -9999.0
    #mTypeID = dhecDB.getMTypeFromObsName(self, obs_type, uom, platform_handle,1)
    sensor_id = dhecDB.sensorExists(self, obs_type, uom, platform_handle)

    if sensor_id:
      rainfallIntensity = self.calcIntensity( platform_handle, sensor_id, date, intervalInMinutes)
      #We do not store radar data that has 0 for precipitation, so we want to make sure not to send
      #-1.0 as the return value as -1.0 indicates no data due to a data problem.
      if rainfallIntensity == -9999.0:
        rainfallIntensity = 0.0
    else:
      self.logger.error("No sensor for: precipitation_radar_weighted_average(in) found for platform: %s" %(platform_handle))
    return rainfallIntensity


  def add_sensor_to_platform(self, platform_handle, sensor_name, uom_name, s_order=1):
    sensor_id = dhecDB.sensorExists(self, sensor_name, uom_name, platform_handle, 1)
    if sensor_id == -1:
      sensor_id = dhecDB.addSensor(self,
                                    sensor_name, uom_name,
                                    platform_handle,
                                    1,
                                    0,
                                    s_order, None, True)
      if sensor_id == -1:
        raise ValueError("Unable to add sensor id for Obs:%s(%s) on platform: %s" % (sensor_name,uom_name, platform_handle))
    m_type_id = dhecDB.getMTypeFromObsName(self, sensor_name, uom_name, platform_handle, 1)

    return sensor_id, m_type_id

  """
  Function: calcAvgWindSpeedAndDir
  Purpose: Wind direction is measured from 0-360 degrees, so around the inflection point trying to do an average can
  have bad results. For instance 250 degrees and 4 degrees are roughly in the same direction, but doing a straight
  average will result in something nowhere near correct. This function takes the speed and direction and converts
  to a vector.
  Parameters:
    platName - String representing the platform name to query
    startDate the date/time to start the average
    endDate the date/time to stop the average
  Returns:
    A tuple setup to contain [0][0] = the vector speed and [0][1] direction average
      [1][0] - Scalar speed average [1][1] - vector direction average with unity speed used.
  """
  def calcAvgWindSpeedAndDir(self, platName, wind_speed_obsname, wind_speed_uom, wind_dir_obsname, wind_dir_uom, startDate, endDate):
    windComponents = []
    dirComponents = []
    vectObj = vectorMagDir();
    #Get the wind speed and direction so we can correctly average the data.
    #Get the sensor ID for the obs we are interested in so we can use it to query the data.
    windSpdId = dhecDB.sensorExists(self, wind_speed_obsname, wind_speed_uom, platName)
    windDirId = dhecDB.sensorExists(self, wind_dir_obsname, wind_dir_uom, platName)
    spd_sql = "SELECT m_date ,m_value FROM multi_obs\
           WHERE sensor_id = %d AND\
           (m_date >= '%s' AND \
           m_date < '%s' ) ORDER BY m_date"\
          %(windSpdId, startDate, endDate)
    if(self.logger):
      self.logger.debug("Wind Speed SQL: %s" % (spd_sql))

    dir_sql = "SELECT m_date ,m_value FROM multi_obs\
           WHERE sensor_id = %d AND\
           (m_date >= '%s' AND \
           m_date < '%s' ) ORDER BY m_date"\
          %(windDirId, startDate, endDate)
    if(self.logger):
      self.logger.debug("Wind Dir SQL: %s" % (dir_sql))
    try:
      windSpdCursor = self.DB.cursor()
      windSpdCursor.execute(spd_sql)
      windDirCursor = self.DB.cursor()
      windDirCursor.execute(dir_sql)
    except sqlite3.Error, e:
      if self.logger:
        self.logger.exception(e)
    else:
      scalarSpd = None
      spdCnt = 0
      for spdRow in windSpdCursor:
        if scalarSpd == None:
          scalarSpd = 0
        scalarSpd += spdRow['m_value']
        spdCnt += 1
        for dirRow in windDirCursor:
          if spdRow['m_date'] == dirRow['m_date']:
            if self.logger:
              self.logger.debug("Calculating vector for Speed(%s): %f Dir(%s): %f" % (spdRow['m_date'], spdRow['m_value'], dirRow['m_date'], dirRow['m_value']))
            #Vector using both speed and direction.
            windComponents.append(vectObj.calcVector(spdRow['m_value'], dirRow['m_value']))
            #VEctor with speed as constant(1), and direction.
            dirComponents.append(vectObj.calcVector(1, dirRow['m_value']))
            break
      #Get our average on the east and north components of the wind vector.
      spdAvg = None
      dirAvg = None
      scalarSpdAvg = None
      vectorDirAvg = None

      #If we have the direction only components, this is unity speed with wind direction, calc the averages.
      if len(dirComponents):
        eastCompAvg = 0
        northCompAvg = 0
        scalarSpdAvg = scalarSpd / spdCnt

        for vectorTuple in dirComponents:
          eastCompAvg += vectorTuple[0]
          northCompAvg += vectorTuple[1]

        eastCompAvg = eastCompAvg / len(dirComponents)
        northCompAvg = northCompAvg / len(dirComponents)
        spdAvg,vectorDirAvg = vectObj.calcMagAndDir(eastCompAvg, northCompAvg)
        if self.logger:
          self.logger.debug("Platform: %s Scalar Speed Avg: %f Vector Dir Avg: %f" % (platName,scalarSpdAvg,vectorDirAvg))

      #2013-11-21 DWR Add check to verify we have components. Also reset the eastCompAvg and northCompAvg to 0
      #before doing calcs.
      #If we have speed and direction vectors, calc the averages.
      if len(windComponents):
        eastCompAvg = 0
        northCompAvg = 0
        for vectorTuple in windComponents:
          eastCompAvg += vectorTuple[0]
          northCompAvg += vectorTuple[1]

        eastCompAvg = eastCompAvg / len(windComponents)
        northCompAvg = northCompAvg / len(windComponents)
        #Calculate average with speed and direction components.
        spdAvg,dirAvg = vectObj.calcMagAndDir(eastCompAvg, northCompAvg)
        if(self.logger):
          self.logger.debug("Platform: %s Vector Speed Avg: %f Vector Dir Avg: %f" % (platName,spdAvg,dirAvg))

      windSpdCursor.close()
      windDirCursor.close()

    return (spdAvg, dirAvg), (scalarSpdAvg, vectorDirAvg)