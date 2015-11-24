"""
Revisions
Author: DWR
Date: 2013-01-02
Function: uomconversionFunctions::measurementConvert
Changes: Removed the xmlTag variable from the except handler.

Date: 2011-07-25
Function: addMeasurement, addMeasurementWithMType
Changes: Added the use of teh row_entry_date column. For code already using this function, if the rowEntryDate 
parameter is not provided, one is created using the current localtime.
"""
import time
from pysqlite2 import dbapi2 as sqlite3      
import psycopg2
import psycopg2.extras
from lxml import etree
from collections import defaultdict  


class recursivedefaultdict(defaultdict):
    def __init__(self):
        self.default_factory = type(self) 

class dbTypes:
  undefined = 0
  SQLite = 1
  PostGRES = 2
"""
Class: qaqcTestFlags
Purpose: This is more of an enumeration class that details out the various quality flags.
"""    
class qaqcTestFlags:

  TQFLAG_DA = 0   #Data Availability               
  TQFLAG_SR = 1   #Sensor Range
  TQFLAG_GR = 2   #Gross Range
  TQFLAG_CR = 3   #Climatological Range
  TQFLAG_RC = 4  #Rate of Change
  TQFLAG_NN = 5  #Nearest Neighbor

  NO_TEST      = 0 # -1 in writeup Unable to perform the test  
  TEST_FAILED  = 1 # 0 in writeup The test failed.
  TEST_PASSED  = 2 #1 in writeup The test passed.
  
  
  NO_DATA           = -9 #the data field is missing a value
  DATA_QUAL_NO_EVAL = 0  #the data quality is not evaluated
  DATA_QUAL_BAD     = 1  #the data quality is bad
  DATA_QUAL_SUSPECT = 2  #the data quality is questionable or suspect
  DATA_QUAL_GOOD    = 3  #the data quality is good
  
  def decodeQCFlag(self, qcFlag):
    qcFlagList = []
    qcFlagList.append( 'Data available ' + self.decodeQCFlagData( int(qcFlag[qaqcTestFlags.TQFLAG_DA]) ) )
    qcFlagList.append( 'Sensor Range ' + self.decodeQCFlagData( int(qcFlag[qaqcTestFlags.TQFLAG_SR]) ) )
    qcFlagList.append( 'Gross Range ' + self.decodeQCFlagData( int(qcFlag[qaqcTestFlags.TQFLAG_GR]) ) )
    qcFlagList.append( 'Climatological Range ' + self.decodeQCFlagData( int(qcFlag[qaqcTestFlags.TQFLAG_CR]) ) )
    qcFlagList.append( 'Rate of Change ' + self.decodeQCFlagData( int(qcFlag[qaqcTestFlags.TQFLAG_RC]) ) )
    qcFlagList.append( 'Nearest Neighbor ' + self.decodeQCFlagData( int(qcFlag[qaqcTestFlags.TQFLAG_NN]) ) )
    return( qcFlagList )

  def decodeQCFlagData(self, qcTestResult):
    if( qcTestResult == qaqcTestFlags.TEST_PASSED ):
      return( 'Test passed' )
    elif( qcTestResult == qaqcTestFlags.TEST_FAILED ):
      return( 'Test failed' )
    else:
      return( 'Test not performed' )

  def decodeQCLevel(self, qcLevel):
    if( qcLevel == qaqcTestFlags.NO_DATA ):
      return( 'No data' )
    elif( qcLevel == qaqcTestFlags.DATA_QUAL_NO_EVAL ):
      return( 'Data quality not evaluated' )
    elif( qcLevel == qaqcTestFlags.DATA_QUAL_BAD ):
      return( 'Data quality bad' )
    elif( qcLevel == qaqcTestFlags.DATA_QUAL_SUSPECT ):
      return( 'Data quality suspect' )
    elif( qcLevel == qaqcTestFlags.DATA_QUAL_GOOD ):
      return( 'Data quality good' )

class statusFlags:
  INACTIVE = 0
  ACTIVE = 1
  TECHNICAL_DIFFICULTY = 2
  OFFLINE = 3
  ARCHIVAL = 4
  DELAYED = 5
  PLANNED = 6


class dbXenia(object):
  def __init__(self):
    self.dbConnection = None
    
  def connect(self, dbFilePath=None, user=None, passwd=None, host=None, dbName=None ):
    #Connecting to a SQLite database
    if(dbFilePath != None):
      self.dbConnection = xeniaSQLite()
      return(self.dbConnection.connect(dbFilePath))
    #Connecting to a PostGres DB
    else:
      self.dbConnection = xeniaPostGres()
      return(self.dbConnection.connect(None, user, passwd, host, dbName))
    return(False)

  def executeQuery(self, sql):
    return(self.dbConnection.executeQuery(sql))
      
class xeniaDB:
  """
  Function: __init__
  Purpose: Initializes the class
  Parameters: None
  Return: None
  """
  def __init__ ( self ):
    self.dbType = dbTypes.undefined
    self.lastErrorMsg   = ''
    self.lastErrorCode  = None
    self.lastErrorFile      = None  
    self.lastErrorLineNo    = None    
    self.lastErrorFunc      = None    
    
    self.DB = None
    
  
  def procTraceback(self):
    import sys
    import traceback
    
    info = sys.exc_info()        
    excNfo = traceback.extract_tb(info[2],1)
    items = excNfo[0]
    self.lastErrorFile = items[0]    
    self.lastErrorLineNo = items[1]    
    self.lastErrorFunc = items[2]    

  def getErrorInfo(self):
    errorMsg = self.lastErrorMsg
    if(self.lastErrorFile != None and self.lastErrorLineNo != None and self.lastErrorFunc != None):
      errorMsg += " Function: %s Line: %s File: %s" %(self.lastErrorFunc,self.lastErrorLineNo,self.lastErrorFile)
    return(errorMsg)
  
  def clearErrorInfo(self):
    self.lastErrorMsg   = ''
    self.lastErrorCode  = None
    self.lastErrorFile      = None  
    self.lastErrorLineNo    = None    
    self.lastErrorFunc      = None    
  """
  Function: connect
  Purpose: Children classes should overload this function to provide the DB specific connection.
  Parameters: 
  Return: 
  """
  def connect(self, dbFilePath=None, user=None, passwd=None, host=None, dbName=None ):
    return(False)
 
  def loadSpatiaLiteLib(self, spatiaLiteLibFile):
    self.DB.enable_load_extension(True)
    sql = 'SELECT load_extension("%s");' % (spatiaLiteLibFile)
    cursor = self.executeQuery(sql)
    if(cursor != None):
      return(True)    
    return(False)
  
  def commit(self):
    try:
      self.DB.commit()
      return(True)
    except psycopg2.Error, E:
      if( E.pgerror != None ):
        self.lastErrorMsg = E.pgerror
      else:
        self.lastErrorMsg = E.message             
      self.lastErrorCode = E.pgcode
      self.procTraceback()
    except sqlite3.Error, e:        
      self.lastErrorMsg = e.args[0]        
      self.procTraceback()
    except Exception, e:        
      self.lastErrorMsg = str(e)       
      self.procTraceback()
    return(False)
  def getMTypeFromObsName(self, obsName, uom, platform, sOrder=1 ):
    sql = "SELECT m_type.row_id FROM m_type "\
          "left join sensor on sensor.m_type_id = m_type.row_id "\
          "left join platform on platform.row_id=sensor.platform_id "\
          "left join m_scalar_type on m_scalar_type.row_id=m_type.m_scalar_type_id "\
          "left join obs_type on obs_type.row_id=m_scalar_type.obs_type_id "\
          "left join uom_type on uom_type.row_id=m_scalar_type.uom_type_id "\
          "WHERE platform.platform_handle='%s' AND obs_type.standard_name='%s' AND uom_type.standard_name='%s' AND sensor.s_order=%d"\
                    %(platform,obsName,uom,sOrder)
    try:                               
      dbCursor = self.executeQuery( sql )
      if( dbCursor != None ):
        row = dbCursor.fetchone()
        mType  = row[0]
        dbCursor.close()
        return( mType )
    except psycopg2.Error, E:
      if( E.pgerror != None ):
        self.lastErrorMsg = E.pgerror
      else:
        self.lastErrorMsg = E.message             
      self.lastErrorCode = E.pgcode
      self.procTraceback()
    except sqlite3.Error, e:        
      self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sql
      self.procTraceback()        
    except Exception, e:        
      self.lastErrorMsg = str(e)
      self.procTraceback()           
    return( None )
    

  """
  Function: rowidExists
  Purpose: Checks to see if a row_id exists for a given SQL query. What this should tell us is
  if what we are looking for, for example a platform, exists in the table.
  Parameters:
    sql is the string containing the SQL statement.
  Returns:
    If found, the row_id, -1 if not found, or None if an error occured.
  """
  def rowidExists(self, sql ):
    dbCursor = self.executeQuery( sql )
    if( dbCursor != None ):
      row = dbCursor.fetchone()
      if( row != None ):
        return(row['row_id'])
      else:
        return( -1 )
    return(None)
 
  """
  Function: obsTypeExists
  Purpose: Checks to see if the passed in obsName exists in the obs_type table.
  Parameters: 
    obsName is the sensor(observation) we are testing for.
  Returns:
    The obs_type(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def obsTypeExists(self, obsName):
    #Does the observation exist in the obs_type table?
    sql = "SELECT row_id FROM obs_type WHERE standard_name = '%s';" % ( obsName )    
    return( self.rowidExists( sql ) )
  
  """
  Function: addObsType
  Purpose: Adds the given obsName into the obs_type table.
    obsName is the sensor(observation) we are adding.
  Returns:
    The obs_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addObsType(self, obsName):
    sql = "INSERT INTO obs_type (standard_name) VALUES ('%s');" %( obsName )
    dbCursor = self.executeQuery(sql)
    if( dbCursor != None ):
      if(self.commit()):
        return( self.obsTypeExists(obsName) )
    return(None)
  
  """
  Function: uomTypeExists
  Purpose: Checks to see if the passed in uom exists in the uom_type table.
  Parameters: 
    uom is the unit of measure we are testing for.
  Returns:
    The uom_type(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def uomTypeExists(self, uom):
    #Check if our UOM exists.        
    sql = "SELECT row_id FROM uom_type WHERE standard_name = '%s';" % ( uom )
    return( self.rowidExists( sql ) )

  """
  Function: addUOMType
  Purpose: Adds the given uom into the uom_type table.
  Parameters:
    uom is the unit of measure we are adding.
  Returns:
    The uom_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addUOMType(self, uom):
    sql = "INSERT INTO uom_type (standard_name) VALUES ('%s');" %( uom )
    dbCursor = self.executeQuery(sql)
    if( dbCursor != None ):
      if(self.commit()):
        return( self.uomTypeExists(uom) )
    return(None)
  
  """
  Function: existsScalarType
  Purpose: Checks to see if the passed in obsTypeID and uomTypeID exists in the scalar_type table. This function is not
  "user friendly" since it requires knowledge of the obs type id and uom type id. Most likely you wouldn't call this directly
  but would be using the addSensor function to do it automagically.
  Parameters: 
    obsTypeID is the row_id of the observation from the obs_type table to check.
    uomTypeID is the row_id of the unit of measure from the uom_type table to check.
  Returns:
    The m_scalar_type_id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def scalarTypeExists(self, obsTypeID, uomTypeID):
    sql = "SELECT row_id FROM m_scalar_type WHERE obs_type_id = %d AND uom_type_id = %d;" % ( obsTypeID,uomTypeID )
    return(self.rowidExists(sql))
  
  """
  Function: addScalarType
  Purpose: Adds a new scalar type into the scalar_type table. This function is not
  "user friendly" since it requires knowledge of the obs type id and uom type id. Most likely you wouldn't call this directly
  but would be using the addSensor function to do it automagically.
  Parameters: 
    obsTypeID is the row_id of the observation from the obs_type table to add.
    uomTypeID is the row_id of the unit of measure from the uom_type table.
  Returns:
    The m_scalar_type_id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addScalarType(self, obsTypeID, uomTypeID):
    sql = "INSERT INTO m_scalar_type (obs_type_id,uom_type_id) VALUES (%d,%d);" %( obsTypeID,uomTypeID )
    dbCursor = self.executeQuery(sql)
    if( dbCursor != None ):
      if(self.commit()):
        return( self.scalarTypeExists(obsTypeID,uomTypeID) )
    return(None)
 
  """
  Function: mTypeExists
  Purpose: Checks to see if the passed in uom exists in the uom_type table.
  Parameters: 
    uom is the unit of measure we are testing for.
  Returns:
    The uom_type(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def mTypeExists(self, scalarID):
    sql = "SELECT row_id FROM m_type WHERE m_scalar_type_id=%d;" %( scalarID )
    return(self.rowidExists(sql))
  
  """
  Function: addMType
  Purpose: Adds a new m_type into the m_type table. This function is not
  "user friendly" since it requires knowledge of the obs type id and uom type id. Most likely you wouldn't call this directly
  but would be using the addSensor function to do it automagically.
  Parameters: 
    scalarID is the row_id of the scalar_type to add.
  Returns:
    The m_type_id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addMType(self, scalarID):    
    sql = "INSERT INTO m_type (m_scalar_type_id) VALUES (%d)" %( scalarID )
    dbCursor = self.executeQuery(sql)
    if( dbCursor != None ):
      if(self.commit()):
        return( self.mTypeExists(scalarID) )
    return(None)
    
  """
  Function: sensorExists
  Purpose: Checks to see if the passed in obsName on the platform.
  Parameters: 
    obsName is the sensor(observation) we are testing for.
    platform is the platform on which we search for the obsName.
    sOrder, if provided specifies the specific sensor if there are multiples of the same on a platform.
  Returns:
    The sensor id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def sensorExists(self, obsName, uom, platform, sOrder=1 ):
      
    sql = "SELECT sensor.row_id as row_id FROM sensor "\
            "left join platform on platform.row_id=sensor.platform_id "\
            "left join m_type on m_type.row_id=sensor.m_type_id "\
            "left join m_scalar_type on m_scalar_type.row_id=m_type.m_scalar_type_id "\
            "left join obs_type on obs_type.row_id=m_scalar_type.obs_type_id "\
            "left join uom_type on uom_type.row_id=m_scalar_type.uom_type_id "\
            "WHERE " \
            "sensor.s_order=%d AND platform.platform_handle='%s' AND obs_type.standard_name='%s' AND uom_type.standard_name='%s'"\
            %(sOrder,platform,obsName,uom)
    return( self.rowidExists( sql ) )
  """
  Function: addSensor
  Purpose: Adds the obsName on the platform given in platformHandle. The flag addObsAndUOM controls whether or not'
    a non existant obsName/uom pair will be added if they do not exist. Used for "simple" sensor entries where there
    is only one measurement being stored.
  Parameters:
    obsName is the sensor we want to add.
    uom is the unit of measure we are adding for the sensor.
    active specifies if the sensor is active or not. 1 = active, 0 = not active
    platformHandle is the name of the platform we are adding the sensor for.
    fixedZ is the height on or below the platform the sensor is installed.
    sOrder if there are multiple sensors of the same type on the platform, this specifies where it is. 1 is closest to the surface.
    mTypeID if we already know the m_type_id, we pass it in via this parameter.
    addObsAndUOM specifies if the obsName or the uom do not exist, if this function will automatically add them. If this flag
      is False and a uom or obsName doesn't exist an error is generated and we return None.
  Returns:
    The uom_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addSensor(self, obsName, uom, platformHandle, active=1, fixedZ=0, sOrder=1, mTypeID=None, addObsAndUOM=False):
    
    #If the sensor already exists, we're done.
    id = self.sensorExists(obsName, uom, platformHandle, sOrder) 
    if(id != -1 and id != None):
      return(id)
      
    #If the mTypeID is passed in, we already have a complete set of obs ids, uoms, scalar types.
    if(mTypeID == None):      
      obsTypeID = self.obsTypeExists( obsName )
      if( obsTypeID == -1 ):
        if( addObsAndUOM ):
          obsTypeID = self.addObsType(obsName)
          #Error occured so return.
          if( obsTypeID == None ):
            self.lastErrorMsg += "\nUnable to add obs_type: %s" % (obsName)
            return(None)
        #If we do not want to add a missing observation type, we must error out.
        else:
          self.lastErrorMsg = "obs_type: %s does not exist. Must be added to obs_type table." %(obsName)
          return(None)
      elif(obsTypeID == None):
        self.lastErrorMsg = "\n obs_type.standard_name: %s does not exist." % (obsName)
          
      #Now let's check if our UOM exists.        
      uomTypeID = self.uomTypeExists( uom )
      if( uomTypeID == -1 ):
        if( addObsAndUOM ):
          uomTypeID = self.addUOMType(uom)
          #Error occured so return.
          if( uomTypeID == None ):
            self.lastErrorMsg += "\nUnable to add uom_type: %s" % (uom)
            return(None)
        #If we do not want to add a missing uom type, we must error out.
        else:
          self.lastErrorMsg = "uom_type: %s does not exist. Must be added to uom_type table." %(uom)
          return(None)
      elif(uomTypeID == None):
        self.lastErrorMsg = "\n uom_type.standard_name: %s does not exist." % (uom)
              
      #Now check the scalar type.
      scalarID = self.scalarTypeExists(obsTypeID,uomTypeID)
      if( scalarID == -1 ):
        scalarID = self.addScalarType(obsTypeID,uomTypeID)
        #Error occured so return.
        if( scalarID == None ):
          self.lastErrorMsg += "\nUnable to add scalar_type with obs_type_id: %d and uom_type_id: %d" % (obsTypeID,uomTypeID)
          return(None)
      elif( scalarID == None ):
        return( None )
      
      #Now we need to add a new m_type
      mTypeID = self.mTypeExists(scalarID)
      if( mTypeID == -1 ):
        mTypeID = self.addMType(scalarID)
        #Error occured so return.
        if( mTypeID == None ):
          self.lastErrorMsg += "\nUnable to add m_type with scalar_type_id: %d" % (scalarID)
          return(None)
      elif( mTypeID == None ):
        return(None)
    
    #Now we can finally add the sensor to the sensor table.
    platformID = self.platformExists(platformHandle)
    if( platformID != None):
      if( platformID != -1 ):
        sql = "INSERT INTO sensor (platform_id,m_type_id,short_name,fixed_z,active,s_order) "\
              "VALUES(%d,%d,'%s',%d,%d,%d)"\
              %(platformID,mTypeID,obsName,fixedZ,active,sOrder)
        dbCursor = self.executeQuery(sql)
        if( dbCursor != None ):
          self.commit()
          return( self.sensorExists(obsName, uom, platformHandle, sOrder) )
    else:
      self.lastErrorMsg = "Platform: %s does not exist. Cannot add sensor." % (platformHandle)
    return( None )  
  
  """
  Function: organizationExists
  Purpose: Checks to see if the organization exists
  Parameters: 
    orgName is the organization short_name we are searching for.
  Returns:
    The row_id if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def organizationExists(self, orgName):
    sql = "SELECT row_id FROM organization WHERE short_name = '%s';" % ( orgName )
    return( self.rowidExists(sql) )
  
  """
  Function: addOrganization
  Purpose: Adds a new organization into the organization table.
  Parameters: 
    orgInfo is a dictionary keyed on the column names of the table. The only required key/values are:
      short_name
    Optional columns are:
      active specifies if the organization is active.         
      long_name a longer name for the organization
      description the description of the org.
      url HTTP address for the org.
      opendap_url Addy for the opendap server.

  Returns:
    The row_id if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addOrganization(self, orgInfo):
    columns = ''
    values = ''
    for column in orgInfo:
      if(len(columns)):
        columns += ","
      columns += column
      if(len(values)):
        values += ","
      if( column == 'active' ):
        values += ("%d" %( int(orgInfo[column]) ))
      else:
        values += ("'%s'" %( orgInfo[column] ))            
    
    if( len(columns) ):
      sql = "INSERT INTO organization (%s) VALUES (%s)" %( columns, values )
      dbCursor = self.executeQuery(sql)
      #If we successfully added the org, let's get it's row_id.
      if( dbCursor != None ):
        try:
          self.DB.commit()
          return( self.organizationExists(orgInfo['short_name']))
        except psycopg2.Error, E:
          if( E.pgerror != None ):
            self.lastErrorMsg = E.pgerror
          else:
            self.lastErrorMsg = E.message             
          self.lastErrorCode = E.pgcode
          self.procTraceback()          
        except sqlite3.Error, e:        
          self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sql        
          self.procTraceback()
        except Exception, e:        
          self.lastErrorMsg = str(e)       
          self.procTraceback()
    return(None)

  """
  Function: platformExists
  Purpose: Checks to see if the platform exists
  Parameters: 
    platformHandle is the handle of the platform we are searching for.
  Returns:
    The row_id if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def platformExists(self, platformHandle):
    sql = "SELECT row_id FROM platform WHERE platform_handle = '%s'" % ( platformHandle )
    return( self.rowidExists(sql) )
  
  """
  Function: addPlatform
  Purpose: Adds a new platform into the platform table.
  Parameters: 
    platformInfo is a dictionary keyed on the column names of the table. The only required key/values are:
      organization_id is the associated organization id.
      platform_handle is the handle for the platform.
    Optional columns are:
      type_id         
      short_name      
      fixed_longitude 
      fixed_latitude  
      active          
      begin_date      
      end_date        
      project_id      
      app_catalog_id  
      long_name       
      description     
      url             
      metadata_id     
  Returns:
    The row_id if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addPlatform(self, platformInfo):
    columns = ''
    values = ''
    for column in platformInfo:
      if(len(columns)):
        columns += ","
      columns += column
      if(len(values)):
        values += ","
      if( column == 'active' or column == 'organization_id' ):
        values += ("%d" %( int(platformInfo[column]) ))
      elif( column == 'fixed_latitude' or column == 'fixed_longitude' ):
        values += ("%f" %( float(platformInfo[column]) ))
      else:
        values += ("'%s'" %( platformInfo[column] ))            
    
    if( len(columns) ):
      sql = "INSERT INTO platform (%s) VALUES (%s)" %( columns, values )
      dbCursor = self.executeQuery(sql)
      #If we successfully added the org, let's get it's row_id.
      if( dbCursor != None ):
        try:
          self.DB.commit()
          return( self.platformExists(platformInfo['platform_handle']))
        except psycopg2.Error, E:
          if( E.pgerror != None ):
            self.lastErrorMsg = E.pgerror
          else:
            self.lastErrorMsg = E.message             
          self.lastErrorCode = E.pgcode
          self.procTraceback()          
        except sqlite3.Error, e:        
          self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sql        
          self.procTraceback()
        except Exception, e:        
          self.lastErrorMsg = str(e)       
          self.procTraceback()
    return(None)

  """
  Function: updateMeasurement
  Purpose: If a measurement for a given time/position already exists and we need to update it. If it doesn't exist, we
  add it.
  """
  def updateMeasurement(self, mTypeID, sensorID, platformHandle, date, lat, lon, z, mValues, sOrder=1, autoCommit=True, rowEntryDate=None, updateDate=None):
    sql = "SELECT row_id FROM multi_obs WHERE m_date='%s' AND sensor_id=%d AND m_type_id=%d AND platform_handle='%s' AND m_lat=%f AND m_lon=%f"\
      % (date, sensorID, mTypeID, platformHandle, lat, lon)
    dbCursor = self.executeQuery(sql)
    #Entry exists, so let's update it.
    addRec = True
    if( dbCursor != None ):
      row = dbCursor.fetchone()
      if( row != None ):
        if(row['row_id'] != None):
          addRec = False      
      dbCursor.close()
      if(addRec == False):
        #There are multiple m_value columns in multi_obs. The values parameter is a list whose index
        #represents the m_value column to be populated.
        m_values = ""
        valID = 1
        for value in mValues:
          if(valID != 1):
            m_values += (",m_value_%d=%f" % (valID,value))
          else:
            m_values += "m_value=%f" % (value)          
          valID += 1
        sql = "UPDATE multi_obs SET %s, row_update_date='%s' WHERE row_id=%d"\
         % (m_values, updateDate, row['row_id'])
        dbCursor = self.executeQuery(sql)
        if( dbCursor != None ):
          if(autoCommit):
            return(self.commit())
          return(True)
        return(False)      
      #Entry doesn't exist, so let's add it.
      else:
        return(self.addMeasurementWithMType(mTypeID, sensorID, platformHandle, date, lat, lon, z, mValues, sOrder, autoCommit, rowEntryDate))
  """
  Function: addMeasurement
  Purpose: Adds a new entry into the multi_obs table.
  """
  def addMeasurementWithMType(self, mTypeID, sensorID, platformHandle, date, lat, lon, z, mValues, sOrder=1, autoCommit=True, rowEntryDate=None):
    #DWR 2011-07-25
    #Added the row_entry_date column. If no date was passed, we create it below to use a localtime.
    columns = "platform_handle,sensor_id,m_type_id,m_date,m_lat,m_lon,m_z,row_entry_date"
    if(rowEntryDate == None):
      rowEntryDate = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    values = "'%s',%d,%d,'%s',%f,%f,%f,'%s'" % (platformHandle,sensorID,mTypeID,date,lat,lon,z,rowEntryDate)
    #There are multiple m_value columns in multi_obs. The values parameter is a list whose index
    #represents the m_value column to be populated.
    valID = 1
    for value in mValues:
      if( valID != 1):
        columns += (",m_value_%d" % ( valID ) )
      else:
        columns += ",m_value"          
      values += (",%f" %(value))
      valID += 1
    sql = "INSERT INTO multi_obs (%s) VALUES (%s)" %( columns, values )
    dbCursor = self.executeQuery(sql)
    if( dbCursor != None ):
      if(autoCommit):
        return(self.commit())
      return(True)
    return(False)

  def addMeasurement(self, obsName, uom, platformHandle, date, lat, lon, z, mValues, sOrder=1, autoCommit=True, rowEntryDate=None ):
    sensorID = self.sensorExists(obsName, uom, platformHandle, sOrder)
    if(sensorID == -1 ):
      self.lastErrorMsg = "Unable to add measurement. Sensor: %s(%s) does not exist on platform: %s. No entry in sensor table." %(obsName,uom,platformHandle)
      return(None) 
    elif(sensorID == None):
      return(None) 
    mTypeID = self.getMTypeFromObsName(obsName, uom, platformHandle, sOrder)
    if(mTypeID == -1 ):
      self.lastErrorMsg = "Unable to add measurement. Sensor: %s(%s) does not exist on platform: %s. No entry in m_type table." %(obsName,uom,platformHandle)
      return(None) 
    elif(mTypeID == None):
      return(None)
    return( self.addMeasurementWithMType( mTypeID, sensorID, platformHandle, date, lat, lon, z, mValues, sOrder, autoCommit,rowEntryDate) )
  
  def getPlatformInfo(self,platformHandle):
    id = self.platformExists(platformHandle)
    if(id != -1 and id != None):
      sql = "SELECT * FROM platform WHERE platform_handle = '%s';" % (platformHandle)
      return(self.executeQuery(sql))
    return(None)
        
  def getDataForSensorID(self,sensorID, startDate, endDate, timeZoneShift):
    return(None)     
  def getObservationDates(self, obsName, platform ):
    return(None)       
  def getObservationsForPlatform(self, platform):
    return(None)      
  def getDataForObs(self, obsName, platform, startDate, endDate, timeZoneShift):
    return(None)
  def getObsDataForPlatform(self, platform, lastNHours = None ):                 
    return(None)

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

class xeniaSQLite(xeniaDB):
  """
  Function: __init__
  Purpose: Initializes the class
  Parameters: None
  Return: None
  """
  def __init__(self):
    self.dbType = dbTypes.SQLite
    xeniaDB.__init__(self)

  """
  Function: connect
  Purpose: Make a connection to the database
  Parameters: 
    dbFilePath is the path to the sqlite database to use.
    user not used
    passwd not used
    host not used
    dbName not used
  Return: 
    True if we successfully connected, otherwise false. Any error info
    is stored in  self.lastErrorMsg
  """
  def connect(self, dbFilePath=None, user=None, passwd=None, host=None, dbName=None ):
    self.dbFilePath = dbFilePath
    try:
      self.DB = sqlite3.connect( self.dbFilePath )
      #This enables the ability to manipulate rows with the column name instead of an index.
      self.DB.row_factory = sqlite3.Row
      return(True)
    except Exception, E:
      self.lastErrorMsg = str(E)                     
      self.procTraceback()
    return(False)

  """
  Function: executeQuery
  Purpose: Executes the sql statement passed in.
  Parameters: 
    sqlQuery is a string containing the query to execute.
  Return: 
    If successfull, a cursor is returned, otherwise None is returned.
  """
  def executeQuery(self, sqlQuery):   
    try:
      dbCursor = self.DB.cursor()
      dbCursor.execute( sqlQuery )        
      return( dbCursor )
    except sqlite3.Error, e:        
      self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sqlQuery        
      self.procTraceback()
    except Exception, E:
      self.lastErrorMsg = str(E)                     
      self.procTraceback()
    return(None)

  def getDataForSensorID(self,sensorID, startDate, endDate, timeZoneShift):
    data = []
    sql = "SELECT multi_obs.m_date,multi_obs.m_value        \
                  FROM multi_obs           \
                  WHERE                    \
                  multi_obs.sensor_id = %d                              AND   \
                  ( m_date >= strftime( '%%Y-%%m-%%dT%%H:00:00',datetime('%s','%d hours') )   AND  \
                  m_date < strftime( '%%Y-%%m-%%dT%%H:00:00', datetime('%s','%d hours') ) ) \
                  ORDER BY multi_obs.m_date ASC;" % ( sensorID, startDate, timeZoneShift, endDate, timeZoneShift );                
    try:                      
      dbCursor = self.executeQuery( sql )
      for row in dbCursor:
        data.append( (row[0],row[1]) )       
      dbCursor.close()
    except sqlite3.Error, e:        
      self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sql        
      self.procTraceback()
    except Exception, E:
      self.lastErrorMsg = str(E)                     
      self.procTraceback()

  def getObsDataForPlatform(self, platform, lastNHours = None ):      
    
    #Do we want to query from a datetime of now back lastNHours?
    dateOffset = ''
    if( lastNHours != None ):
      dateOffset = "m_date > strftime('%%Y-%%m-%%dT%%H:%%M:%%S', 'now','-%d hours') AND" % (lastNHours)
        
    sql= "SELECT m_date \
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
          WHERE %s multi_obs.platform_handle = '%s' AND qc_level IS NULL AND sensor.row_id IS NOT NULL\
          ORDER BY m_date DESC" \
          % (dateOffset,platform)
    try:
      dbCursor = self.executeQuery( sql )       
      return( dbCursor )
    except sqlite3.Error, e:        
      self.lastErrorMsg = 'SQL ERROR: ' + e.args[0] + ' SQL: ' + sql        
      self.procTraceback()
    except Exception, E:
      self.lastErrorMsg = str(E)                           
      self.procTraceback()
    return(None)


class xeniaPostGres(xeniaDB):
  """
  Function: __init__
  Purpose: Initializes the class
  Parameters: None
  Return: None
  """
  def __init__(self):
    xeniaDB.__init__(self)
    self.dbType = dbTypes.PostGRES

  """
  Function: connect
  Purpose: Make a connection to the database
  Parameters: 
    dbFilePath is not used for a PostGres connection
    user is the user name to attempt to login with on the database.
    passwd the password for the user acccount on the database
    host is the address the host is located on. If locale, you still need to provide 127.0.0.1
    dbName is the databse name we want to connect to.
  
  Return: 
    True if we successfully connected, otherwise false. Any error info
    is stored in  self.lastErrorMsg
  """
  def connect(self, dbFilePath=None, user=None, passwd=None, host=None, dbName=None ):
    try:
      connstring = "dbname=%s user=%s" % (dbName,user)
      if(host != None):
        connstring += " host=%s" %(host)
      if(passwd != None):
        connstring += " password=%s" % (passwd)
        
      #connstring = "dbname=%s user=%s host=%s password=%s" % ( dbName,user,host,passwd) 
      self.DB = psycopg2.connect( connstring )
      return(True)       
    except psycopg2.Error, E:
      if( E.pgerror != None ):
        self.lastErrorMsg = E.pgerror
      else:
        self.lastErrorMsg = E.message             
      self.lastErrorCode = E.pgcode
      self.procTraceback()
    except Exception, E:
      self.lastErrorMsg = str(E)                     
      self.procTraceback()
    return(False)

  """
  Function: executeQuery
  Purpose: Executes the sql statement passed in.
  Parameters: 
    sqlQuery is a string containing the query to execute.
  Return: 
    If successfull, a cursor is returned, otherwise None is returned.
  """
  def executeQuery(self, sqlQuery):
    try:
      dbCursor = self.DB.cursor(cursor_factory=psycopg2.extras.DictCursor)     
      dbCursor.execute( sqlQuery )        
      return( dbCursor )
    except psycopg2.Error, E:
      if( E.pgerror != None ):
        self.lastErrorMsg = E.pgerror
      else:
        self.lastErrorMsg = E.message             
      self.lastErrorCode = E.pgcode
      self.procTraceback()
    except Exception, E:
      self.lastErrorMsg = str(E)                     
      self.procTraceback()
    return( None )
  
  def getCurrentPlatformStatus(self,platformHandle):
    sql = "SELECT active FROM platform WHERE platform_handle='%s';"\
          %(platformHandle)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      row = dbCursor.fetchone()
      if(row != None):
        return(row['active'])
    return(None)
          
  def setPlatformStatus(self, platformHandle,status):
    #Get the platform id and the organization id
    sql = "SELECT row_id,organization_id FROM platform WHERE platform_handle='%s';" %(platformHandle)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      row = dbCursor.fetchone()
      if(row != None):
        platformId = row['row_id']
        OrgId = row['organization_id']
        
        #Get row entry date, we use local time for it.
        rowEntryDate = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime())
        #Begin date is entered as GMT time.
        gmtDate = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        
        #Platform is not in an active state, so these should be the first entries into the
        #platform_status and archive table.
        if(status != statusFlags.ACTIVE):
          #Update the platform_status table to reflect the new status.
          sql = "INSERT INTO platform_status "\
          "(platform_id,organization_id,row_entry_date,begin_date,status,platform_handle) "\
          "values (%d,%d,'%s','%s',%d,'%s');"\
          %(platformId,OrgId,rowEntryDate,gmtDate,status,platformHandle)
          statusCursor = self.executeQuery(sql)
          if(statusCursor == None):
            return(False)
          self.commit()
          statusCursor.close()
          #Now add entry into the platform_status_archive table
          sql="INSERT INTO platform_status_archive "\
              "(platform_id,organization_id,row_entry_date,begin_date,status) "\
              "values(%d,%d,'%s','%s',%d);"\
              %(platformId,OrgId,rowEntryDate,gmtDate,status)
          statusCursor = self.executeQuery(sql)
          if(statusCursor == None):
            return(False)
          self.commit()
          statusCursor.close()
        #Platform is becoming active again. We get rid of the entry in the platform_status
        #table then add teh end date into the platform_status_field 
        else:
          #Get current platform_status info to move over to the archive.
          sql = "SELECT author,reason FROM platform_status WHERE platform_id=%d;" %(platformId)
          
          statusCursor = self.executeQuery(sql)
          if(statusCursor == None):
            return(False)
          row = statusCursor.fetchone()
          author = ''
          reason = ''
          if(row != None):
            author = row['author']
            reason = row['reason']
          statusCursor.close()
          
          sql = "DELETE FROM platform_status WHERE platform_id=%d;" %(platformId)
          statusCursor = self.executeQuery(sql)
          if(statusCursor == None):
            return(False)
          self.commit()
          statusCursor.close()
          
          sql="UPDATE platform_status_archive SET end_date='%s',row_update_date='%s',author='%s',reason='%s'"\
              "WHERE platform_id=%d AND end_date IS NULL;"\
              %(gmtDate,rowEntryDate,author,reason,platformId)
          statusCursor = self.executeQuery(sql)
          if(statusCursor == None):
            return(False)
          self.commit()
          statusCursor.close()
          
        #Now update the active field in the platform table.
        sql = "UPDATE platform SET active=%d WHERE platform_handle='%s';"\
        %(status,platformHandle)
        statusCursor = self.executeQuery(sql)
        if(statusCursor == None):
          return(False)
        self.commit()
        statusCursor.close()
        
        
      return(True)
    
    return(False)    

  def getPlatformStatus(self, platformHandle):
    status = None
    sql = "SELECT  to_char(begin_date,'YYYY-MM-DD HH24:MM:SS') as begin_date,reason FROM platform_status "\
      "WHERE platform_handle='%s' AND end_date IS NULL;"\
      %(platformHandle)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      row = dbCursor.fetchone()
      if(row != None):
        status = {}
        status['begin_date'] = row['begin_date'] 
        reason = "No data received from platform."
        if(row['reason'] != None):
          reason = row['reason']
        status['reason'] = reason
    return(status)
         
  def getObsDataForPlatform(self, platform, lastNHours = None ):      
    
    #Do we want to query from a datetime of now back lastNHours?
    dateOffset = ''
    if( lastNHours != None ):
      dateOffset = "m_date >  date_trunc('hour',( SELECT timezone('UTC', now()-interval '%d hours' ) ) ) AND" % (lastNHours)
        
    sql= "SELECT m_date \
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
          WHERE %s multi_obs.platform_handle = '%s' AND qc_level IS NULL AND sensor.row_id IS NOT NULL\
          ORDER BY m_date DESC" \
          % (dateOffset,platform)
    try:
      dbCursor = self.executeQuery( sql )       
      return( dbCursor )
    except Exception, E:
      self.lastErrorMsg = str(E)                     
      self.procTraceback()
      
    return(None)
            
"""
Class: uomconversionFunctions
Purpose: Uses a conversion XML file to look up a from units of measurement and to units of measurement conversion 
routine. If one is found, will evaluate the function and return the result. The XML file needs to be formated with 
valid python code.
"""
class uomconversionFunctions:
  
  """
  Function: __init__
  Purpose: Initializes the class
  Parameters: 
    xmlConversionFile is the full path to the XML file to use for the conversions.
  """
  def __init__(self, xmlConversionFile=None, parseFile=True):
    self.xmlConversionFile = xmlConversionFile
    self.xmlTree = None
    if(parseFile):
      self.xmlTree = etree.parse(self.xmlConversionFile)
      
    
  def setXMLConversionFile(self, xmlConversionFile):
    self.xmlConversionFile = xmlConversionFile
    self.xmlTree = etree.parse(self.xmlConversionFile)
    
  """
  Function: measurementConvert
  Purpose: Attempts to find a conversion formula using the passed in fromUOM and toUOM variables.
  Parameters:
    value is the floating point number to try and convert.
    fromUOM is the units of measurement the value is currently in.
    toUOM is the units of measurement we want to value to be converted to.
  Return:
    If a conversion routine is found, then the converted value is returned, otherwise None is returned.
  """
  def measurementConvert(self, value, fromUOM, toUOM):
    try:
      if(self.xmlTree == None):
        self.xmlTree = etree.parse(self.xmlConversionFile)
      
      convertedVal = ''
      conversionString = ''
      xmlTag = "//unit_conversion_list/unit_conversion[@id=\"%s_to_%s\"]/conversion_formula" % (fromUOM, toUOM)
      unitConversion = self.xmlTree.xpath(xmlTag)
      if( len(unitConversion) ):     
        conversionString = unitConversion[0].text
        conversionString = conversionString.replace( "var1", ("%f" % value) )
        convertedVal = float(eval( conversionString ))
        return(convertedVal)
      
    except Exception, E:
      import traceback
      #DWR 2013-01-02
      #Removed the xmlTag variable since it is not needed and might not exist here.
      msg = "Value: %f fromUOM: %s toUOM: %s xmlTag: %s \n%s" %(value, fromUOM, toUOM, conversionString, traceback.format_exc())
      print(msg)
      
    return(None)

  """
  Function: getUnits
  Purpose: For a given conversion from/to pair, this returns the to units to display.
  Parameters: 
    fromUOM is the units of measurement the value is currently in.
    toUOM is the units of measurement we want to value to be converted to.
  Return:
    If a units string is found, then the string is returned, otherwise None is returned.
  
  """
  def getUnits(self, fromUOM, toUOM):
    if(self.xmlTree == None):
      self.xmlTree = etree.parse(self.xmlConversionFile)
    
    #xmlTree = etree.parse(self.xmlConversionFile)
    
    xmlTag = "//unit_conversion_list/unit_conversion[@id=\"%s_to_%s\"]/units" % (fromUOM, toUOM)
    units = self.xmlTree.xpath(xmlTag)
    if( len(units) ):     
      units = units[0].text
      return(units)
    return(None)

  """
  Function: getConversionUnits
  Purpose: Given a unit of measurement in a differing measurement system and the desired measurement system, returns uom in the desired measurement system.
  Parameters:
    uom is the current unit of measurement we want to convert.
    uomSystem is the desired measurement system we want to conver the uom into.
  Return:
    if the uomSystem is found and the uom is in the uomSystem, returns the conversion uom
  """
  def getConversionUnits(self, uom, uomSystem):
    if( uomSystem == 'en' ):
      if( uom == 'm'):
        return('ft')
      elif( uom == 'm_s-1'):
        return('mph')
      elif( uom == 'celsius' ):
        return('fahrenheit')
      elif(uom == 'cm_s-1'):
        return('mph')
      elif(uom == 'mph'):
        return('knots')
    else:
      if( uom == 'ft'):
        return('m' )
      if( uom == 'inches'):
        return('millimeter' )
      elif( uom == 'mph'):
        return('m_s-1')
      elif( uom == 'fahrenheit' ):
        return('celsius')
      elif(uom == 'mph'):
        return('cm_s-1')
      elif(uom == 'knots'):
        return('mph')
      elif(uom == 'inches_mercury'):
        return('mb')
      
    return('')

  def getAbbreviatedObservationName(self, obsName):
    if(self.xmlTree == None):
      self.xmlTree = etree.parse(self.xmlConversionFile)
    xmlTag = "//unit_conversion_list/observation[@id=\"%s\"]/abbreviation" % (obsName)
    abbreviation = self.xmlTree.xpath(xmlTag)
    if( len(abbreviation) ):     
      abbreviation = abbreviation[0].text
      return(abbreviation)
    return(None)

  def getDisplayObservationName(self, obsName):
    if(self.xmlTree == None):
      self.xmlTree = etree.parse(self.xmlConversionFile)
    xmlTag = "//unit_conversion_list/observation[@id=\"%s\"]/displayName" % (obsName)
    display = self.xmlTree.xpath(xmlTag)
    if( len(display) ):     
      display = display[0].text
      return(display)
    return(None)

  def getXeniaUOMName(self, uomName):
    if(self.xmlTree == None):
      self.xmlTree = etree.parse(self.xmlConversionFile)
    xmlTag = "//unit_conversion_list/unit[@id=\"%s\"]/xenia_unit" % (uomName)
    xeniaUOM = self.xmlTree.xpath(xmlTag)
    if(len(xeniaUOM)):     
      xeniaUOM = xeniaUOM[0].text
      return(xeniaUOM)
    return(None)
    