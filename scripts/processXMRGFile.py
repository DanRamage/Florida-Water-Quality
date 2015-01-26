import time
import logging
import logging.handlers
import getRemoteFiles
from xmlConfigFile import xmlConfigFile


class configSettings(xmlConfigFile):
  def __init__(self, xmlConfigFilename):
    try:
      #Call parents __init__
      xmlConfigFile.__init__(self, xmlConfigFilename)
      
      #Log file settings
      self.logFile = self.getEntry('//logging/logDir')
      self.maxBytes = self.getEntry('//logging/maxBytes')
      if(self.maxBytes == None):
        self.maxBytes = 100000
      else:
        self.maxBytes = int(self.maxBytes)
        
      self.backupCount = self.getEntry('//logging/backupCount')
      if(self.backupCount == None):
        self.backupCount = 5
      else:
        self.backupCount = int(self.backupCount)

      self.dbSettings = self.getDatabaseSettings()
      self.spatiaLiteLib = self.getEntry('//database/db/spatiaLiteLib')

      self.baseURL = self.getEntry('//xmrgData/baseURL')
      #This tag is used to help further refine the files we process. For instance, hourly xmrg files are prepended
      #with xmrg whereas the 6hr and 24hr files aren't. So we could use this to ignore those.
      self.fileNameFilter = self.getEntry('//xmrgData/fileNameFilter')   
      self.xmrgDLDir = self.getEntry('//xmrgData/downloadDir')

      self.loggerName = self.getEntry('//xmrgData/loggerName')
      if(self.loggerName == None):
        self.loggerName = 'xmrg_logger'
      
    except Exception, e:
      import traceback     
      print(traceback.print_exc())
      sys.exit(- 1)

class processXMRGData(object):  
  def __init__(self, xmlConfigFile):
    try:
  
      self.configSettings = configSettings( xmlConfigFile )
      
      #Create our logging object.
      self.logger = None
      if(self.configSettings.logFile !=None):
        self.logger = logging.getLogger(self.configSettings.loggerName)
        self.logger.setLevel(logging.DEBUG)
        # create formatter and add it to the handlers
        formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(lineno)d,%(message)s")
    
        #Create the log rotation handler.
        handler = logging.handlers.RotatingFileHandler( self.configSettings.logFile, "a", self.configSettings.maxBytes, self.configSettings.backupCount )
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)    
        self.logger.addHandler(handler)
        # add the handlers to the logger
        self.logger.info('Log file opened')
            
      if( self.configSettings.dbSettings['dbName'] == None ):
        if(self.logger != None):
          self.logger.error( 'ERROR: //database/db/name not defined in config file. Terminating script' )
          sys.exit(-1)                     
      if(self.logger != None):
        self.logger.debug( 'Database path: %s' % (self.configSettings.dbSettings['dbName']) )
                 
      if(self.configSettings.spatiaLiteLib == None ):
        if(self.logger != None):
          self.logger.error( 'ERROR: //database/db/spatiaLiteLib not defined in config file. Terminating script' )
          sys.exit(-1)           
      if(self.configSettings.baseURL == None):
        if(self.logger != None):
          self.logger.error( "//xmrgData/baseURL not defined, cannot continue." )
          sys.exit(-1)
      if(self.configSettings.xmrgDLDir == None):
        if(self.logger != None):
          self.configSettings.xmrgDLDir  = './';         
          self.logger.debug( "//xmrgData/downloadDir not provided, using './'." )
        
    except Exception, E:
      self.lastErrorMsg = str(E)
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])
      if(self.logger != None):
        self.logger.error(self.lastErrorMsg)
      else:
        print( self.lastErrorMsg )
      sys.exit(-1)

  """
  Function: __del__
  Purpose: Destructor. Used to make sure the logger gets completely shutdown. 
  """
  def __del__(self):
    #Cleanup the logger.
    if( self.logger != None ):
      logging.shutdown()

    
  """
  Function: buildXMRGFilename
  Purpose: Given the desiredDateTime, creates an XMRG filename for that date/time period.
  Parameters:
    desiredDateTime is the desired data and time for the xmrg file we want to download.
  Returns:
    A string containing the xmrg filename.
  """    
  def buildXMRGFilename(self, desiredDateTime, convertToUTC=True):
    desiredTime=time.strptime(desiredDateTime, "%Y-%m-%dT%H:00:00")
    #Internally we stores date/times as localtime, however the xmrg remote files are stamped as UTC
    #times so we have to convert.
    if(convertToUTC):
      desiredTime=time.mktime(desiredTime)
      desiredTime=time.gmtime(desiredTime)

    #Hourly filename format is: xmrgMMDDYYYYHHz.gz WHERE MM is month, DD is day, YYYY is year
    #HH is UTC hour     
    hour = time.strftime( "%H", desiredTime)
    date = time.strftime( "%m%d%Y", desiredTime)
    fileName = 'xmrg%s%sz.gz' % ( date,hour )
    return(fileName)
  
  """
  Function: getXMRGFile
  Purpose: Attempts to download the file name given in fileName.
  Parameters: 
    fileName is the name of the xmrg file we are trying to download.
  Returns:
  
  """
  def getXMRGFile(self, fileName):
    return(self.remoteFileDL.getFile( fileName, None, False))

  """
  Function: getLatestHourXMRGData
  Purpose: Attempts to download the current hours XMRG file.
  Parameters: None
  Returns: 
    True if successful, otherwise False.
  """
  def getLatestHourXMRGData(self):    
    try: 
      self.remoteFileDL = getRemoteFiles.remoteFileDownload( self.configSettings.baseURL, self.configSettings.xmrgDLDir, 'b', False, None, True)
            
      #The latest completed hour will be current hour - 1 hour(3600 seconds).
      hr = time.time()-3600
      latestHour = time.strftime( "%Y-%m-%dT%H:00:00", time.localtime(hr))
      
      fileName = self.buildXMRGFilename(latestHour)
      #Now we try to download the file.
      fileName = self.getXMRGFile(fileName)
      if( fileName != None ):
        self.logger.info( "Processing XMRG File: %s" %(fileName))
        xmrg = xmrgFile( self.configSettings.loggerName )
        xmrg.openFile( fileName )
        return( self.processXMRGFile( xmrg ) )
      else:
        self.logger.error( "Unable to download file: %s" %(fileName))
          
    except Exception, E:
      self.lastErrorMsg = str(E) 
      info = sys.exc_info()        
      excNfo = traceback.extract_tb(info[2],1)
      items = excNfo[0]
      self.lastErrorMsg += " File: %s Line: %d Function: %s" % (items[0],items[1],items[2])      
      self.logger.error( self.lastErrorMsg )
    return(False)
  
  """
  Function: processXMRGFile
  Purpose: Override this function to do whatever specific processing/saving that needs to be done to the file.
  Parameters:
    xmrgFile is the open xmrgFile object to be processed.
  Returns:
    True if succesful, otherwise False.
  """
  def processXMRGFile(self,xmrgFile):
    return(False)
