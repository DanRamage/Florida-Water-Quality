"""
Revisions
Author: DWR
Date: 2012/06/21
Changes: xmrgCleanup class added. Used to organize the XMRG files in a given directory into an archival
  directory.
"""
import os
import os.path
import sys
import array
import struct
import csv
import time
import re
import shutil
import logging
import logging.handlers
#from collections import defaultdict  
import optparse
import math
import gzip
from numpy import zeros
from pysqlite2 import dbapi2 as sqlite3      
import datetime


class hrapCoord(object):
  def __init__(self, column=None, row=None):
    self.column = column
    self.row    = row
class LatLong(object):
  def __init__(self,lat=None,long=None):
    self.latitude = lat
    self.longitude= long

def getCollectionDateFromFilename(fileName):
  #Parse the filename to get the data time.
  (directory,filetime) = os.path.split( fileName )
  (filetime,ext) = os.path.splitext( filetime )
  #Let's get rid of the xmrg verbage so we have the time remaining.
  #The format for the time on these files is MMDDYYY sometimes a trailing z or for some historical
  #files, the format is xmrg_MMDDYYYY_HRz_SE. The SE could be different for different regions, SE is southeast.
  #24 hour files don't have the z, or an hour

  dateformat = "%m%d%Y%H"
  #Regexp to see if we have one of the older filename formats like xmrg_MMDDYYYY_HRz_SE
  fileParts = re.findall("xmrg_\d{8}_\d{1,2}", filetime)
  if(len(fileParts)):
    #Now let's manipulate the string to match the dateformat var above.
    filetime = re.sub("xmrg_", "", fileParts[0])
    filetime = re.sub("_","", filetime)
  else:
    if(filetime.find('24hrxmrg') != -1):
      dateformat = "%m%d%Y"
    filetime = filetime.replace('24hrxmrg', '')
    filetime = filetime.replace('xmrg', '')
    filetime = filetime.replace('z', '')
  #Using mktime() and localtime() is a hack. The time package in python doesn't have a way
  #to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
  #I want instead of brining in the calender package which has the conversion.
  secs = time.mktime(time.strptime( filetime, dateformat ))
  #secs -= offset
  filetime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(secs) )

  return(filetime)

"""
  Class: xmrgFile
  Purpose: This class processes a NOAA XMRG binary file.
"""
class xmrgFile:
  """
    Function: init
    Purpose: Initalizes the class.
    Parameters: None
    Return: None
  """
  def __init__(self, loggerName=None):
    self.logger = None
    if( loggerName != None ):
      self.logger = logging.getLogger(loggerName)
      self.logger.debug("creating an instance of xmrgFile")
    
    self.fileName = ''
    self.lastErrorMsg = ''
    self.headerRead = False
    
    self.earthRadius = 6371.2;
    self.startLong   = 105.0;
    self.startLat    = 60.0;
    self.xmesh       = 4.7625;
    self.meshdegs    = (self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh
  
  """
  Function: Reset
  Purpose: Prepares the xmrgFile object for reuse. Resets various variables and closes the currently open file object.
  Parameters: None
  Return: None
  """
  def Reset(self):
    self.fileName = ''
    self.lastErrorMsg = ''
    self.xmrgFile.close()
  
  """
  Function: openFile
  Purpose: Attempts to open the file given in the filePath string. If the file is compressed using gzip, this will uncompress
    the file as well.
  Parameters:
    filePath is a string with the full path to the file to open.
  Return:
    True if successful, otherwise False.
  """
  def openFile(self, filePath):
    self.fileName = filePath
    self.compressedFilepath = ''
    retVal = False
    try:
      #Is the file compressed? If so, we want to uncompress it to a file for use.
      #The reason for not working with the GzipFile object directly is it is not compatible
      #with the array.fromfile() functionality.
      if( self.fileName.rfind('gz') != -1):
        self.compressedFilepath = self.fileName
        #SPlit the filename from the extension.
        parts = self.fileName.split('.')
        self.fileName = parts[0] 
        self.xmrgFile = open( self.fileName, mode = 'wb' )
        zipFile = gzip.GzipFile( filePath, 'rb' )
        contents = zipFile.read()
        self.xmrgFile.writelines(contents)
        self.xmrgFile.close()

      self.xmrgFile = open( self.fileName, mode = 'rb' )
      retVal = True
    except Exception, E:
      import traceback      
      self.lastErrorMsg = traceback.format_exc()
      if(self.logger != None ):
        self.logger.error(self.lastErrorMsg)
      else:
        print(self.lastErrorMsg) 
   
    return(retVal)
  
  """
 Function: cleanUp
 Purpose: Called to delete the XMRG file that was just worked with. Can delete the uncompressed file and/or 
  the source compressed file. 
 Parameters:
   deleteFile if True, will delete the unzipped binary file.
   deleteCompressedFile if True, will delete the compressed file the working file was extracted from.
  """
  def cleanUp(self,deleteFile,deleteCompressedFile):
    self.xmrgFile.close()
    if(deleteFile):
      os.remove(self.fileName)
    if(deleteCompressedFile and len(self.compressedFilepath)):
      os.remove(self.compressedFilepath)
    return
    
  """
  Function: readFileHeader
  Purpose: For the open file, reads the header. Call this function first before attempting to use readRow or readAllRows.
    If you don't the file pointer will not be at the correct position.
  Parameters: None
  Returns: True if successful, otherwise False.
  """
  def readFileHeader( self ):
    try:
      #Determine if byte swapping is needed.
      #From the XMRG doc:
      #FORTRAN unformatted records have a 4 byte integer at the beginning and
      #end of each record that is equal to the number of 4 byte words
      #contained in the record.  When reading xmrg files through C using the
      #fread function, the user must account for these extra bytes at the
      #beginning and end of each  record.
      
      #Original header is as follows
      #4 byte integer for num of 4 byte words in record
      #int representing HRAP-X coord of southwest corner of grid(XOR)
      #int representing HRAP-Y coord of southwest corner of grid(YOR)
      #int representing HRAP grid boxes in X direction (MAXX)
      #int representing HRAP grid boxes in Y direction (MAXY)
      header = array.array('I')
      #read 6 bytes since first int is the header, next 4 ints are the grid data, last int is the tail. 
      header.fromfile( self.xmrgFile, 6)
      self.swapBytes= 0
      #Determine if byte swapping is needed
      if( header[0] != 16 ):
        self.swapBytes = 1
        header.byteswap()
      
      self.XOR = header[1]    #X Origin of the HRAP grid     
      self.YOR = header[2]    #Y origin of the HRAP grid
      self.MAXX = header[3]   #Number of columns in the data 
      self.MAXY = header[4]   #Number of rows in the data 
      
      #reset the array
      header = array.array('I')
      #Read the fotran header for the next block of data. Need to determine which header type we'll be reading
      header.fromfile( self.xmrgFile, 1 )
      if( self.swapBytes ):
        header.byteswap()
        
      self.fileNfoHdrData = '' 
      byteCnt = header[0]  
      unpackFmt = ''
      hasDataNfoHeader = True
      srcFileOpen = False  
      #Header for files written 1999 to present.
      if( byteCnt == 66 ):
        #The info header has the following layout
        #Operating system: char[2]
        #user id: char[8]
        #saved date: char[10]
        #saved time: char[10]
        #process flag: char[20]
        #valid date: char[10]
        #valid time: char[10]
        #max value: int
        #version number: float
        unpackFmt += '=2s8s10s10s8s10s10sif'
        #buf = array.array('B')
        #buf.fromfile(self.xmrgFile,66)
        #if( self.swapBytes ):
        #  buf.byteswap()
          
        buf = self.xmrgFile.read(66)
        
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        srcFileOpen = True
      #Files written June 1997 to 1999  
      elif( byteCnt == 38 ):
        if( self.swapBytes ):
          unpackFmt += '>'
        unpackFmt += '=10s10s10s8s'
        buf = self.xmrgFile.read(38)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        srcFileOpen = True
        
      #Files written June 1997 to 1999. I assume there was some bug for this since the source
      #code also was writing out an error message.  
      elif( byteCnt == 37 ):
        if( self.swapBytes ):
          unpackFmt += '>'
        unpackFmt += '=10s10s10s8s'
        buf = self.xmrgFile.read(37)
        self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
        srcFileOpen = True
        
      #Files written up to June 1997, no 2nd header.  
      elif( byteCnt == ( self.MAXX * 2 ) ):
        if( self.swapBytes ):
          unpackFmt += '>'
        if( loggerName != None ):
          self.logger.info( "Reading pre-1997 format" )
        else:
          print( "Reading pre-1997 format" )        
        srcFileOpen = True
        #File does not have 2nd header, so we need to reset the file point to the point before we
        #did the read for the 2nd header tag.
        self.xmrgFile.seek( 24, os.SEEK_SET )
        hasDataNfoHeader = False
      
      #Invalid byte count.
      else:
        self.lastErrorMsg = 'Header is unknown format, cannot continue.'
        return( False )
      
      #If the file we are reading was not a pre June 1997, we read the tail int, 
      #should be equal to byteCnt
      if( hasDataNfoHeader ): 
        header = array.array('I')
        header.fromfile( self.xmrgFile, 1 )
        if( self.swapBytes ):
          header.byteswap()        
        if( header[0] != byteCnt ):
          self.lastErrorMsg = 'ERROR: tail byte cnt does not equal head.'
          return( False )
          
      if( srcFileOpen ):
        self.headerRead = True
        return( True )

    except Exception, E:
      import traceback      
      self.lastErrorMsg = traceback.format_exc()
      
      if( self.logger != None ):
        self.logger.error(self.lastErrorMsg)
      else:
        print(self.lastErrorMsg)
    
    return( False )      
  
  """
  Function: readRecordTag
  Purpose: Reads the tag that surrounds each record in the file.
  Parameters: None
  Return: An integer dataArray with the tag data if read, otherwise None.
  """
  def readRecordTag(self):
    dataArray= array.array('I')
    dataArray.fromfile( self.xmrgFile, 1 )
    if( self.swapBytes ):
      dataArray.byteswap();
    #Verify the header for this row of data matches what the header specified.
    #We do MAXX * 2 since each value is a short.
    if( dataArray[0] != (self.MAXX*2) ):
      self.lastErrorMsg = 'Trailing tag Byte count: %d for row: %d does not match header: %d.' %( dataArray[0], row, self.MAXX )
      return( None )
    return(dataArray)
  
  """
  Function: readRow
  Purpose: Reads a single row from the file.
  Parameters: None'
  Returns: If successful a dataArray containing the row values, otherwise None.
  """
  def readRow(self):
    #Read off the record header
    tag = self.readRecordTag()
    if( tag == None ):
      return(None)
    
    #Read a columns worth of data out
    dataArray= array.array('h')
    dataArray.fromfile( self.xmrgFile, self.MAXX )
    #Need to byte swap?
    if( self.swapBytes ):
      dataArray.byteswap();    

    #Read off the record footer.
    tag = self.readRecordTag()
    if( tag == None ):
      return(None)

    return( dataArray )
  
  """
  Function: readAllRows
  Purpose: Reads all the rows in the file and stores them in a dataArray object. Data is stored in self.grid.
  Parameters: None
  Returns: True if succesful otherwise False.
  
  """
  def readAllRows(self):
    #Create a integer numeric array(from numpy). Dimensions are MAXY and MAXX.
    self.grid = zeros([self.MAXY,self.MAXX],int)
    for row in range( self.MAXY ):    
      dataArray= self.readRow()
      if( dataArray == None ):
        return(False)
      col = 0                    
      for val in dataArray:            
        self.grid[row][col] = val
        col+=1
              
    return(True)
  
  """
  Function: inBBOX
  Purpose: Tests to see if the testLatLong is in the bounding box given by minLatLong and maxLatLong.
  Parameters:
    testLatLong is the lat/long pair we are testing.
    minLatLong is a latLong object representing the bottom left corner.
    maxLatLong is a latLong object representing the upper right corner.
  Returns:
    True if the testLatLong is in the bounding box, otherwise False.
  """  
  def inBBOX(self, testLatLong, minLatLong, maxLatLong):
    inBBOX = False
    if( ( testLatLong.latitude >= minLatLong.latitude and testLatLong.longitude >= minLatLong.longitude ) and
        ( testLatLong.latitude < maxLatLong.latitude and testLatLong.longitude < maxLatLong.longitude ) ):
      inBBOX = True
    return( inBBOX )
  

  """
  Function: hrapCoordToLatLong
  Purpose: Converts the HRAP grid point given in hrapPoint into a latitude and longitude.
  Parameters:  
    hrapPoint is an hrapPoint object that defines the row,col point we are converting.
  Returns:
    A LatLong() object with the converted data.
  """
  def hrapCoordToLatLong(self, hrapPoint ):
    latLong     = LatLong()
        
    x = hrapPoint.column - 401.0;
    y = hrapPoint.row - 1601.0;
    rr = x * x + y * y
    #gi = ((self.earthRadius * (1.0 + math.sin(self.tlat))) / self.xmesh)
    #gi *= gi
    #gi = ((self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh)
    gi = self.meshdegs * self.meshdegs
    #latLong.latitude = math.asin((gi - rr) / (gi + rr)) * self.raddeg
    latLong.latitude = math.degrees(math.asin((gi - rr) / (gi + rr)))
    
    #ang = math.atan2(y,x) * self.raddeg
    ang = math.degrees(math.atan2(y,x))
    
    if(ang < 0.0):
      ang += 360.0;
    latLong.longitude = 270.0 + self.startLong - ang;
    
    if(latLong.longitude < 0.0):
      latLong.longitude += 360.0;
    elif(latLong.longitude > 360.0):
      latLong.longitude -= 360.0;
    
    return( latLong )

  """
  Function: latLongToHRAP
  Purpose: Converts a latitude and longitude into an HRAP grid point.
  Parameters:  
    latLong is an latLong object that defines the point we are converting.
    roundToNearest specifies if we want to round the hrap point to the nearest integer value.
    adjustToOrigin specifies if we want to adjust the hrap point to the origin of the file.
  Returns:
    A LatLong() object with the converted data.
  """
  def latLongToHRAP(self, latLong, roundToNearest=False, adjustToOrigin=False):
    flat = math.radians( latLong.latitude )
    flon = math.radians( abs(latLong.longitude) + 180.0 - self.startLong )
    r = self.meshdegs * math.cos(flat)/(1.0 + math.sin(flat))
    x = r * math.sin(flon)
    y = r * math.cos(flon)
    hrap = hrapCoord( x + 401.0, y + 1601.0 )
    
    #Bounds checking
    if( hrap.column > ( self.XOR + self.MAXX ) ):
      hrap.column = self.XOR + self.MAXX
    if( hrap.row > (self.YOR + self.MAXY) ):
      hrap.row = self.YOR + self.MAXY
    if( roundToNearest ):
      hrap.column = int( hrap.column - 0.5 ) 
      hrap.row = int( hrap.row - 0.5 )
    if( adjustToOrigin ):
      hrap.column -= self.XOR 
      hrap.row -= self.YOR
    
    return(hrap)
  
    
  def biLinearInterpolatePoint(self, x, y, z0, z1, z2, z3):
    z = None
    # z3------z2
    # |       |
    # |       |
    # z0------z1
    #b1 + b2x + b3y + b4xy 
    #b1 = z0
    #b2 = z1-z0
    #b3 = z3-z0
    #b4 = z0-z1-z3+z2
    
    b1 = z0
    b2 = z1 - z0
    b3 = z3 - z0
    b4 = z0 - z1 - z3 + z2
    z = b1 + (b2*x) + (b3*y) + (b4*x*y)

    return(z)
  
  """
  Function: getCollectionDateFromFilename
  Purpose: Given the filename, this will return a datetime string in the format of YYYY-MM-DDTHH:MM:SS.
  Parameters:
    fileName is the xmrg filename to parse the datetime from.
  Return:
    A string representing the date and time in the form: YYYY-MM-DDTHH:MM:SS
  """
  def getCollectionDateFromFilename(self, fileName):
    #Parse the filename to get the data time.
    (directory,filetime) = os.path.split( fileName )
    (filetime,ext) = os.path.splitext( filetime )
    #Let's get rid of the xmrg verbage so we have the time remaining.
    #The format for the time on these files is MMDDYYY sometimes a trailing z or for some historical
    #files, the format is xmrg_MMDDYYYY_HRz_SE. The SE could be different for different regions, SE is southeast.     
    #24 hour files don't have the z, or an hour
    
    dateformat = "%m%d%Y%H" 
    #Regexp to see if we have one of the older filename formats like xmrg_MMDDYYYY_HRz_SE
    fileParts = re.findall("xmrg_\d{8}_\d{1,2}", filetime)
    if(len(fileParts)):
      #Now let's manipulate the string to match the dateformat var above.
      filetime = re.sub("xmrg_", "", fileParts[0])
      filetime = re.sub("_","", filetime)
    else:
      if(filetime.find('24hrxmrg') != -1):
        dateformat = "%m%d%Y"
      filetime = filetime.replace('24hrxmrg', '')
      filetime = filetime.replace('xmrg', '')
      filetime = filetime.replace('z', '')
    #Using mktime() and localtime() is a hack. The time package in python doesn't have a way
    #to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
    #I want instead of brining in the calender package which has the conversion.
    secs = time.mktime(time.strptime( filetime, dateformat ))
    #secs -= offset
    filetime = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(secs) )
    
    return(filetime)
  
class xmrgDB(object):
  """
  Function: __init__
  Purpose: Initializes the object
  Parameters: None
  Returns: None
  
  """  
  def __init__(self):
    self.db = None
    self.lastErrorMsg = ''
  """
  Function: connect
  Purpose: Connects to the sqlite database file passed in dbFilepath.
  Parameters:
    dbFilepath is the fully qualified path to the sqlite database.
  Returns: True if we successfully connected to the database, otherwise False.
  If an exception occured, the stack trace is written into self.lastErrorMsg.
  
  """  
  def connect(self, dbFilepath, spatiaLiteLibFile=''):
    try:
      self.db = sqlite3.connect( dbFilepath )
      #This enables the ability to manipulate rows with the column name instead of an index.
      self.db.row_factory = sqlite3.Row
      #If the path to the spatialite package was provided, attempt to load the extension.
      if(len(spatiaLiteLibFile)):
        self.db.enable_load_extension(True)
        sql = 'SELECT load_extension("%s");' % (spatiaLiteLibFile)
        cursor = self.executeQuery(sql)
        cursor.close()
        if(cursor != None):
          return(True)
        else:
          self.lastErrorMsg = "Failed to load SpatiaLite library: %s. Cannot continue." %(spatiaLiteLibFile)    
    except Exception, E:
      import traceback        
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      
      self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                      exceptionValue,
                                      exceptionTraceback)))     
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
      dbCursor = self.db.cursor()
      dbCursor.execute( sqlQuery )        
      return( dbCursor )
    except sqlite3.Error, e:
      import traceback        
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      
      self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                      exceptionValue,
                                      exceptionTraceback)))     
      del e
    except Exception, E:
      import traceback        
      exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
      
      self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                      exceptionValue,
                                      exceptionTraceback)))
      del E     
    return(None)

  
  """
  Function: cleanPrecipRadar
  Purpose: This function will remove all data older the olderThanDate from the precipitation_radar table.
  Parameters:
    olderThanDate is the comparison date to use.
  Return: 
    True if successful, otherwise False.
  """
  def cleanUp(self, olderThanDate):
    sql = "DELETE FROM precipitation_radar WHERE collection_date < strftime('%%Y-%%m-%%dT%%H:%%M:%%S', '%s');" % (olderThanDate)
    dbCursor = self.executeQuery(sql)
    if(dbCursor != None):
      try:
        self.db.commit()
        dbCursor.close()
        return(True)  
      except sqlite3.Error, e:
        import traceback        
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        
        self.lastErrorMsg = (repr(traceback.format_exception(exceptionType, 
                                        exceptionValue,
                                        exceptionTraceback)))     
    return(False)  

  """
  Function: buildPolygonString
  Purpose: Takes a list of latitudes/longitudes representing a polygon and builds a GIS POLYGON string.
  Parameters:
    polygonPtList is a list of x,y tuples which forms the polygon we use to determine the intersection with the 
      radar polygons.
  Return:
    A GIS POLYGON string that can be used for a SQL spatial query.
  """
  def buildPolygonString(self, polygonPtList):
    if(len(polygonPtList)):
      points = ''
      for point in polygonPtList:
        point = point.lstrip()
        point = point.rstrip()
        point = point.split(' ')
        if(len(points)):
          points += ',' 
        buf = ('%s %s' % (point[0], point[1]))
        points += buf
      return('POLYGON((%s))' % (points))
    return('')
    
  """
  Function: getRadarDataForBoundary
  Purpose: For the given rain gauge(boundaryName), this function will return the radar data that is in that POLYGON.
  Parameters:
    boundaryPolygon is a list of x,y tuples which forms the polygon we use to determine the intersection with the 
      radar polygons.
    strtTime is the datetime to begin the search
    endTime is the datetime to end the search.
  Return:
    Database cursor with the results if query is successful, otherwise None.
  """
  def getRadarDataForBoundary(self, boundaryPolygon,strtTime,endTime):
    polyString = self.buildPolygonString(boundaryPolygon)
    sql = "SELECT ogc_fid,latitude,longitude,precipitation,geom FROM precipitation_radar \
            WHERE\
            (collection_date >= '%s' AND collection_date <= '%s') AND\
            Intersects( Geom, \
                        GeomFromText('%s'))"\
            %(strtTime,endTime,polyString)
    #print(sql)
    return(self.executeQuery(sql))
  
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
  def calculateWeightedAvg(self, boundaryPolygon, startTime, endTime):
    weighted_avg = -9999
    polyString = self.buildPolygonString(boundaryPolygon)
    #Get the percentages that the intersecting radar grid make up of the watershed boundary.      
    sql = "SELECT * FROM(\
           SELECT (ST_Area(ST_Intersection(radar.geom,GeomFromText('%s')))/ST_Area(GeomFromText('%s'))) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar \
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                Intersects(radar.geom, GeomFromText('%s')))"\
                %(polyString, polyString, startTime, endTime, polyString)
    dbCursor = self.executeQuery(sql)
    #print(sql)        
    if(dbCursor != None):
      total = 0.0
      date = ''
      cnt = 0
      for row in dbCursor:
        percent = row['percent']
        precip = row['precipitation']
        if(percent != None and precip != None):
          total += (percent * precip)
        else:
          print("Row: %d percent or precip is None" %(cnt))
        cnt += 1
      dbCursor.close()
      if(cnt > 0):
        weighted_avg = total
    else:
      weighted_avg = None
    return(weighted_avg)
  
  def calculateWeightedAvg2(self, polygonKey, startTime, endTime):
    weighted_avg = -9999
    #Get the percentages that the intersecting radar grid make up of the watershed boundary.      
    sql = "SELECT * FROM(\
           SELECT (ST_Area(ST_Intersection(radar.geom,(SELECT the_geom FROM watershed_boundary WHERE name = '%s')))/ST_Area((SELECT the_geom FROM watershed_boundary WHERE name = '%s'))) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar \
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                Intersects(radar.geom, (SELECT the_geom FROM watershed_boundary WHERE name = '%s')))"\
                %(polygonKey, polygonKey, startTime, endTime, polygonKey)
    dbCursor = self.executeQuery(sql)
    #print(sql)        
    if(dbCursor != None):
      total = 0.0
      date = ''
      cnt = 0
      for row in dbCursor:
        percent = row['percent']
        precip = row['precipitation']
        if(percent != None and precip != None):
          total += (percent * precip)
        else:
          print("Row: %d percent or precip is None" %(cnt))
        cnt += 1
      dbCursor.close()
      if(cnt > 0):
        weighted_avg = total
    else:
      weighted_avg = None
    return(weighted_avg)

  """
  Function: vacuumDB
  Purpose: Cleanup the database. 
  Parameters: None
  Return: True if successful, otherwise False.
  """    
  def vacuumDB(self):
    try:
      sql = "VACUUM;"
      dbCursor = self.db.cursor()
      dbCursor.execute(sql)    
      dbCursor.close()
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
          VALUES('%s','%s',%f,%f,%f,GeomFromWKB(X'%s',4326));" \
          %(datetime, filetime, latitude, longitude, val, grid_polygon.wkb_hex)
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
                        GeomFromWKB(X'%s'))"\
            % (start_time, end_time, boundary_polygon.wkb_hex)
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
           SELECT (Area(Intersection(radar.geom,GeomFromWKB(X'%s', 4326)))/Area(GeomFromWKB(X'%s', 4326))) as percent,\
                   radar.precipitation as precipitation\
           FROM precipitation_radar radar\
           WHERE radar.collection_date >= '%s' AND radar.collection_date <= '%s' AND\
                Intersects(radar.geom, GeomFromWKB(X'%s', 4326)))"\
                %(boundary_polygon.wkb_hex, boundary_polygon.wkb_hex, start_time, end_time, boundary_polygon.wkb_hex)
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
Purpose: This class is a utlity class to perform a variety of tasks on a directory with XMRG files. For the most part
it takes files for a certain year/month and moves them to a directory corresponding to that year/month to help keep 
our main processing directory cleaner and make it easier to find files.
"""
class xmrgCleanup(object):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters:
    xmrgSrcDir - THe directory where the XMRG files currently are stored.
    xmrgDestDir - The directory to move the XMRG files
    logger - Flag, if true, logging is done, otherwise it is not.
  """
  def __init__(self, xmrgSrcDir, xmrgDestDir, logger=True):
    self.srcDirectory = xmrgSrcDir
    self.destDirectory = xmrgDestDir
    if(logger):
      self.logger = logging.getLogger(type(self).__name__)
      
  
  """
  Function: organizeFilesIntoDirectories
  Purpose: Organizes the XMRG files by moving them from the self.srcDirectory into the self.destDirectory
    having a further directory structure of \Year\Abbreviated Month.
  Parameters:
    filesOlderThan - datetime.datetime object that specifies the maximum date to keep in the self.srcDirectory. All
      older files are moved.
  """
  def organizeFilesIntoDirectories(self, filesOlderThan=None, organizeByYear=True):
    fileList = os.listdir(self.srcDirectory)
    
    for fileName in fileList:      
      fullPath = "%s/%s" % (self.srcDirectory,fileName)       
      #Verify we have a file, if not, pull it from the list.
      if(os.path.isfile(fullPath) != True):
        fileList.remove(fileName)
        if(self.logger):
          self.logger.debug("%s is not a file, removing." % (fileName))
        continue       
      
      if(fileName.find('xmrg') == -1):
        fileList.remove(fileName)
        if(self.logger):
          self.logger.debug("%s is not a valid XMRG file, removing." % (fileName))
        continue
    xFile = xmrgFile()
    for fileName in fileList:
      collectionDate = xFile.getCollectionDateFromFilename(fileName)
      collectionDate = datetime.datetime.strptime(collectionDate, "%Y-%m-%dT%H:%M:%S")
      moveFile = True
      if(filesOlderThan):
        if(collectionDate > filesOlderThan):
          moveFile = False
          
      if(moveFile):
        try:
          #Directory structure is /year/abbreviated month: /2012/Aug
          archiveDir = "%s/%s/%s" % (self.destDirectory,collectionDate.year, collectionDate.strftime("%b"))
          #If the year directory doesn't exist, we create it.
          if(os.path.exists(archiveDir) != True):
            os.makedirs(archiveDir)
          srcFullPath = "%s/%s" % (self.srcDirectory, fileName)
          destFullPath = "%s/%s" % (archiveDir, fileName)
          shutil.move(srcFullPath, destFullPath)
        except Exception,e:
          if(self.logger):
            self.logger.exception(e)
          
if __name__ == '__main__':   
  try:
    parser = optparse.OptionParser()
    parser.add_option("-d", "--DatabaseFile", dest="databaseFile",
                      help="Full path to the database used to store the imported file." )
    parser.add_option("-s", "--SpatialiteLib", dest="spatialiteLib",
                      help="Full path to the spatialite library. For windows this will be a DLL, for Linux a shared object file." )
    parser.add_option("-f", "--XMRGFile", dest="xmrgFile",
                      help="The XMRG file to process." )
    parser.add_option("-b", "--BBOX", dest="bbox",
                      help="The bounding box to use to select the area of interest from the source XMRG file and store into the database.\
                            If not provided, the entire XMRG file is imported." )
    parser.add_option("-0", "--StoreDryPrecipCells", dest="storeDryPrecipCells", action= 'store_true',
                      help="If set, when importing the XMRG file, cells that had precipitation of 0 will be stored in the database.")
    parser.add_option("-p", "--Polygon", dest="polygon",
                      help="Polygon of interest to use for querying against the radar data." )
    
    (options, args) = parser.parse_args()
    #if( options.xmlConfigFile == None ):
    #  parser.print_usage()
    #  parser.print_help()
    #  sys.exit(-1)
      
    db = xmrgDB()
    if(db.connect(options.databaseFile, options.spatialiteLib) != True):
      print("Unable to connect to database: %s, cannot continue" %(options.databaseFile))


    #Each long/lat pair is seperated by a comma, so let's braek up the input into the pairs.
    bboxParts = options.bbox.split(',')
    minLatLong = LatLong()
    maxLatLong = LatLong()
    #Each long/lat pair is seperated by a space.
    pairs = bboxParts[0].split(' ')
    minLatLong.longitude = float(pairs[0]) 
    minLatLong.latitude = float(pairs[1])
    pairs = bboxParts[1].split(' ')
    maxLatLong.longitude = float(pairs[0]) 
    maxLatLong.latitude = float(pairs[1])
          
    #Open the XMRG file and process the contents, storting the data into the database.
    dataFile = xmrgFile()
    dataFile.openFile(options.xmrgFile)
    if( dataFile.readFileHeader() ):     
      print( "File Origin: X %d Y: %d Columns: %d Rows: %d" %(dataFile.XOR,dataFile.YOR,dataFile.MAXX,dataFile.MAXY))
      if( dataFile.readAllRows() ):
        #In the binary file, the data is stored as hundreths of mm, if we want to write the data as 
        #inches , need to divide by 2540.
        dataConvert = 100.0 
        dataConvert = 25.4 * dataConvert 

        #This is the database insert datetime.           
        datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )
        #Parse the filename to get the data time.
        (directory,filetime) = os.path.split( dataFile.fileName )
        (filetime,ext) = os.path.splitext( filetime )
        filetime = dataFile.getCollectionDateFromFilename(filetime)


        #Flag to specifiy if any non 0 values were found. No need processing the weighted averages 
        #below if nothing found.
        rainDataFound=False 
        #If we are using a bounding box, let's get the row/col in hrap coords.
        llHrap = None
        urHrap = None
        startCol = 0
        startRow = 0
        #If we are using a bounding box to clip out the input data we are interested in, convert those
        #lat/longs into the HRAP grid to set where we start our import.
        if( minLatLong != None and maxLatLong != None ):
          llHrap = dataFile.latLongToHRAP(minLatLong,True,True)
          urHrap = dataFile.latLongToHRAP(maxLatLong,True,True)
          startCol = llHrap.column
          startRow = llHrap.row
        recsAdded = 0
        for row in range(startRow,dataFile.MAXY):
          for col in range(startCol,dataFile.MAXX):
            val = dataFile.grid[row][col]
            #If there is no precipitation value, or the value is erroneous 
            if( val <= 0 ):
              if(options.storeDryPrecipCells):
                val = 0
              else:
                continue
            else:
              val /= dataConvert
              
            hrap = hrapCoord( dataFile.XOR + col, dataFile.YOR + row )
            latlon = dataFile.hrapCoordToLatLong( hrap )                                
            latlon.longitude *= -1
            saveToDB = False
            if( minLatLong != None and maxLatLong != None ):
              if( dataFile.inBBOX( latlon, minLatLong, maxLatLong ) ):
                saveToDB = True
            else:
              saveToDB = True
            if(saveToDB):
              #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
              #that has each point in the grid for a given point.                  
              hrapNewPt = hrapCoord( dataFile.XOR + col, dataFile.YOR + row + 1)
              latlonUL = dataFile.hrapCoordToLatLong( hrapNewPt )
              latlonUL.longitude *= -1
              hrapNewPt = hrapCoord( dataFile.XOR + col + 1, dataFile.YOR + row)
              latlonBR = dataFile.hrapCoordToLatLong( hrapNewPt )
              latlonBR.longitude *= -1
              hrapNewPt = hrapCoord( dataFile.XOR + col + 1, dataFile.YOR + row + 1)
              latlonUR = dataFile.hrapCoordToLatLong( hrapNewPt )
              latlonUR.longitude *= -1
              wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))"\
                    %(latlon.longitude, latlon.latitude,
                      latlonUL.longitude, latlonUL.latitude, 
                      latlonUR.longitude, latlonUR.latitude, 
                      latlonBR.longitude, latlonBR.latitude, 
                      latlon.longitude, latlon.latitude, 
                      )
              sql = "INSERT INTO precipitation_radar \
                    (insert_date,collection_date,latitude,longitude,precipitation,geom) \
                    VALUES('%s','%s',%f,%f,%f,GeomFromText('%s',4326));" \
                    %( datetime,filetime,latlon.latitude,latlon.longitude,val,wkt)
              cursor = db.executeQuery( sql )
              #Problem with the query, since we are working with transactions, we have to rollback.
              if( cursor != None ):
                recsAdded += 1
              else:
                print(db.lastErrorMsg)
                db.lastErrorMsg = None
                db.db.rollback()
        #Commit the inserts.    
        db.db.commit()
        print('Added: %d records to database.' % (recsAdded))

        #Now let's take the polygon of interest, find out all the radar cells that intersect it, then 
        #calculate a weighted average.
        polygonPtList = options.polygon.split(',')
        radarCursor = db.getRadarDataForBoundary(polygonPtList, filetime, filetime)
        if(radarCursor != None):
          for row in radarCursor:
            print( "Longitude: %s Latitude: %s PrecipValue: %s" % (row['longitude'],row['latitude'],row['precipitation']))
        weightedAvg = db.calculateWeightedAvg(polygonPtList, filetime, filetime)
        print("Weighted Average: %f" %(weightedAvg))
  except Exception, E:
    import traceback
    print( traceback.print_exc() )
        