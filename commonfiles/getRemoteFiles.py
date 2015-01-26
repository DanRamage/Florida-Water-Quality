"""
Revision: 
Date: 2011-07-26
Function: remoteFileDownload.__init__
Changes: Added logger object reference. Throughout the code, whereever a print was used, a test is now used to see
  if we have a valid logger object, if so that is used.
"""
#import sys
from urllib2 import Request, urlopen, URLError, HTTPError
import time
import re
import optparse


####################################################################################################################
# Class will get the html doc file listing for a URL and then allow a file by file pulling down of the 
# data. Can be made "smart" by using the fetch log which will keep track of what files have been pulled and
# can be further refined to see if the modification date has changed.
####################################################################################################################
class remoteFileDownload:
  def __init__ ( self, baseURL, destDir, fileMode='b', useFetchLog=False, fetchLogDir=None, log=False, logger=None ):
    self.baseURL = baseURL      #The base url we will be pulling the file(s) from.
    #Is the URL an HTTP or FTP? The "screen scraping" is different for each.
    self.httpAddy = True
    if( self.baseURL.find('ftp') != -1 or self.baseURL.find('FTP') != -1 ):
      self.httpAddy = False
    
    self.log       = log            #1 to log out message, 0 otherwise
    self.destDir = destDir      #Directory on local machine file(s) to be created.
    self.fileMode= fileMode     #File open() mode "a" = ASCII "b=binary"
    self.strLastError = ''            #If an error occured, this string will contain it.
    self.useFetchLog = useFetchLog #1 to create a log file to keep track of what file(s) we have downloaded.
    self.fetchLogDir = fetchLogDir #Directory to store the fetch log files
    
    #DWR 2011-07-26
    self.logger = logger
    
  def SetBaseURL(self, baseURL ):
    self.baseURL = baseURL      #The base url we will be pulling the file(s) from.
    #Is the URL an HTTP or FTP? The "screen scraping" is different for each.
    self.httpAddy = True
    if( self.baseURL.find('ftp') != -1 or self.baseURL.find('FTP') != -1 ):
      self.httpAddy = False
  
  ####################################################################################################################
  #Function: checkForNewFiles
  #Purpose: Connects to the url given in the init function and attempts to pull out a file listing using a regexp.
  #Params:
  #  filter is a user passed in regexp to replace the default("href\s*=[\s|\"]*(.*?)[\s|\"]") one.
  #Return:
  #  A list containing the file names.
  ####################################################################################################################
    
  #Taken from http://www.techniqal.com/blog/2008/07/31/python-file-read-write-with-urllib2/
  def checkForNewFiles(self, filter):
    if( self.httpAddy ):
      return( self.httpCheckForNewFiles(filter) )
    else:
      return(self.ftpCheckForNewFiles(filter))
    return(None)

  def ftpCheckForNewFiles(self, filter):      
    fileList = ['']
    try:
      #create the url and the request
      strUrl = self.baseURL
      req = Request(strUrl)
      # Open the url
      f = urlopen(req)
      #The list we get back ends up looking like:
      #-rwxrwxr-x    1 524      376         62948 Feb 05 00:04 WTEO_20090204v10001.nc
      #-rwxrwxr-x    1 524      376         62948 Feb 05 18:17 WTEO_20090204v10002.nc
      #So we want to split it up and get down to just the filename.
      dirList = f.read()
      dirList = dirList.splitlines()
      for row in dirList:
        parts = row.split(' ')
        fileList.append(parts[-1])
    #handle errors
    except HTTPError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::ftpCheckForNewFiles: %s %s" %(e.code, strUrl)
      #print "HTTP Error:",e.code , strUrl
    except URLError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::ftpCheckForNewFiles: %s %s" %(e.reason, strUrl)
      #print "URL Error:",e.reason , strUrl
    except Exception, e:  
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::ftpCheckForNewFiles: %s" % (str(e))
      #print "Error:",e.reason
    
    if(len(self.strLastError ) and self.logger == None):
      self.logMsg( self.strLastError )
        
    return( fileList )

  def httpCheckForNewFiles(self, filter):      
    fileList = ['']
    try:
      #create the url and the request
      strUrl = self.baseURL
      req = Request(strUrl)
      # Open the url
      f = urlopen(req)
      HTMLDirList = f.read()
      list = []        
      if( len( filter ) == 0 ):
        list = re.findall(r'href\s*=[\s|\"]*(.*?)[\s|\"]', HTMLDirList)
      else:
        list = re.findall(r'%s'%(filter), HTMLDirList)
      for file in list:
        print("%s" % (file))
        #There are <a href="?C=N;O=D"> or <a href="/rcoos/"> strings in the HTMLDirList that represent the column names
        #in the listing. In the above regexp, these are viewed as valid since we are simply filtering
        #on anything falling in between <href=>. This loop further refines the filter to get rid
        #of the bogus hrefs.
        if( (re.match( r'[\?C=.]', file ) == None) and    
            (re.match( r'[/]', file ) == None )):
          fileList.append(file)
     
    #handle errors
    except HTTPError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::httpCheckForNewFiles: %s %s" %(e.code, strUrl)
      #print "HTTP Error:",e.code , strUrl
    except URLError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::httpCheckForNewFiles: %s %s" %(e.reason, strUrl)
      #print "URL Error:",e.reason , strUrl
    except Exception, e:  
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::httpCheckForNewFiles: : %s" % (str(e))
      #print "Error:",e.reason
    
    if( len(self.strLastError ) and self.logger == None):
      self.logMsg( self.strLastError )
        
    return( fileList )

  ####################################################################################################################
  #Function: writeFetchLogFile
  #Purpose: Writes a fetched log file into the directory, fetchLogDir, passed into the init function. File naming convention
  #  takes the remote file name, strips the extension and adds a .log extension. The modification date for the server file
  #  is stored in the fetch log file.
  #  
  #Params:
  #  fileName is the fetch log we are looking for. The path is added from the fetchLogDir used in the init function.
  #  dateTime is the modded time as pulled from the HTML header and convereted to seconds UTC.
  #Return:
  #  1 if file is created, otherwise 0.
  ####################################################################################################################
  def writeFetchLogFile(self, fileName, dateTime):
    try:
      strFilePath = self.fetchLogDir + fileName
      fetchLog = open( strFilePath, 'w' )      
      fetchLog.write( ( "%d\n" % dateTime ) )
      if(self.logger != None):
        self.logger.debug("Creating fetchlog: %s Modtime: %d" %(strFilePath,dateTime))
      else:
        self.logMsg( "writeFetchLogFile::Creating fetchlog: %s Modtime: %d" % (strFilePath,dateTime) )
      return(1)
    except IOError, e:
      self.strLastError = str(e)
      self.logMsg( self.strLastError )
    return(0)
  
  ####################################################################################################################
  #Function: checkFetchLogFile
  #Purpose: Searches for a fetched log file in the directory, fetchLogDir, passed into the init function. File naming convention
  #  takes the remote file name, strips the extension and adds a .log extension. 
  #Params:
  #  fileName is the fetch log we are looking for. The path is added from the fetchLogDir used in the init function.
  #Return:
  #  Returns the file modded date if there, otherwise -1.
  ####################################################################################################################
  def checkFetchLogFile(self, fileName):
    ModDate = -1
    try:
      strFilePath = self.fetchLogDir + fileName

      LogFile = open( strFilePath, 'r' )
      ModDate = LogFile.readline()     
      if( len(ModDate) ):
        ModDate = float( ModDate )
      if(self.logger != None):
        self.logger.debug("Fetchlog %s exists. Modtime: %d" % (strFilePath,ModDate))
      else:
        self.logMsg( "checkFetchLogFile::Fetchlog %s exists. Modtime: %d" % (strFilePath,ModDate) )

    except IOError, e:
      self.strLastError = str(e)
      if(self.logger != None):
        self.logger.exception(e)
      else:  
        if( e.errno != 2 ):
          self.logMsg( self.strLastError )
        else:
          self.logMsg( "checkFetchLogFile::Fetchlog: %s does not exist" % (strFilePath) )

    return(ModDate)

  ####################################################################################################################
  #Function: getFile
  #Purpose: Pulls the file given by remoteFileName onto the local machine and stores it in the directory
  #  destDir passed into the init function. User can also specify a destFileName to name the file something different
  #  when storing on the local machine. If the user specified to use the fetch log, setup in the init, this function
  #  checks to see if the file already exists. If it does, and the user has not specified to further check the server side
  #  files modification date, the file is not pulled. If the user does wish to determine if the file has been modded, it will be
  #  downloaded if it is newer than the current local file.
  #Params:
  #  remoteFileName is the remote file that is to be downloaded.
  #  destFileName is the optional filename to save the remoteFileName to. If you wish to keep the remoteFileName, pass an empty string.
  #  compareModDate set to 1 will compare the remoteFileName mod date with the fetch log files date, if it exists. If the remoteFileName is
  #    newer, it will be downloaded. 0 is a simple check to see if the file has already been downloaded by looking to see if the fetch log
  #    file for that file exists.
  #Return:
  #  The filepath of the filedownloaded, otherwise an empty string.
  ####################################################################################################################
  def getFile( self, remoteFileName, destFileName, compareModDate ):
    retVal = None
    try:    
      if(self.logger != None):
        self.logger.info("Processing file: %s from URL : %s" % (remoteFileName,self.baseURL))
      else:
        self.logMsg( '-----------------------------------------------------------------' )
        self.logMsg( 'getFile::Processing file: ' + remoteFileName + ' from URL: ' + self.baseURL )
      
      url = self.baseURL + remoteFileName
      req = Request(url)   
      htmlFile = urlopen(req)
      
      downloadFile = 1
      if( self.useFetchLog ):
        strFetchLogFile = ''
        if( len(destFileName) ):
          strFetchLogFile = destFileName
        else:
          strFetchLogFile = remoteFileName
                
        #We just want the file name, not the extension, so split it up.
        fileParts = strFetchLogFile.split( "." )
        strFetchLogFile = fileParts[0] + '.log'
        
        info = htmlFile.info()
        ModDate = None
        if( 'Last-Modified' in htmlFile.headers ):
          ModDate = htmlFile.headers['Last-Modified']
          #Convert the time into a seconds notation to make comparisons easier.
          date = time.strptime( ModDate, '%a, %d %b %Y %H:%M:%S %Z' )
          ModDate = time.mktime(date)
        else:
          ModDate = time.time()
            
        logFileDate = self.checkFetchLogFile(strFetchLogFile)
        
        writeFetchLogFile = 0
        #If logFileDate the fetch log file doesn't exist, so we need to create it.
        if( logFileDate == -1 ):
          writeFetchLogFile = 1    
          
        if( compareModDate and ModDate != None ):
          if( ModDate <= logFileDate ):
            downloadFile = 0
          else:            
            if(self.logger != None):
              self.debug("File modification date is now: %.1f, previous mod date: %.1f" % (ModDate, logFileDate))
            else:
              self.logMsg( ("getFile::File modification date is now: %.1f, previous mod date: %.1f" % (ModDate, logFileDate)) )
            writeFetchLogFile = 1
            
        #Not comparing file mod dates, but need to see if we had grabbed that file already. 
        else:
          #If we got a logFileDate back from the check above, we already have the file and don't
          #need to dl it.
          if( logFileDate != -1 ):
            downloadFile = 0
            
        if( writeFetchLogFile ):       
          self.writeFetchLogFile(strFetchLogFile, ModDate)
        
      if( downloadFile ):
        strDestFilePath = self.destDir + remoteFileName
        if(destFileName):
          strDestFilePath = self.destDir + destFileName
          
        if(self.logger != None):
          self.logger.debug("Downloading file: %s" %(strDestFilePath))
        else:
          self.logMsg( 'getFile::Downloading file: ' +  strDestFilePath )
        DestFile = open(strDestFilePath, "w" + self.fileMode)
        #Write to our local file
        DestFile.write(htmlFile.read())
        DestFile.close()
        retVal = strDestFilePath
        
    #handle errors
    except HTTPError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::getFile: %s %s" %(e.code, url)
    except URLError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::getFile: %s %s" %(e.reason, url)
    except Exception, e:  
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.strLastError = "ERROR::getFile::Error: %s" %(str(e))

    if( len(self.strLastError ) and self.logger == None):
      self.logMsg( self.strLastError )
      self.strLastError = ""
    
    if(self.logger == None):  
      self.logMsg( '-----------------------------------------------------------------' )
    
    return(retVal)
    
  def getFiles( self, fileList, fileFilter ):
    try:
      for fileName in fileList :    
        if( re.match( fileFilter, fileName ) != None ):
          
          url = self.baseURL + fileName
          req = Request(url)   
          htmlFile = urlopen(req)
          
          info = htmlFile.info()
          ModDate = htmlFile.headers['Last-Modified']
          
          strDestFilePath = self.destDir + fileName
            
          DestFile = open(strDestFilePath, "w" + self.fileMode)
          #Write to our local file
          DestFile.write(htmlFile.read())
          DestFile.close()
          
      return( 1 )

    #handle errors
    except HTTPError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.logMsg("ERROR::getFile: %s %s" %(e.code, url))
      return(-1)
    except URLError, e:
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.logMsg("ERROR::getFile: %s %s" %(e.reason, url))
      return(-1)
    except Exception, e:  
      if(self.logger != None):
        self.logger.exception(e)
      else:
        self.logMsg("ERROR::getFile::Error: %s" % (str(e)))
      return(-1)
        
    return(0)
  ####################################################################################################################
  #Function: logMsg
  #Purpose: Logs messages if the user passed a 1 in for the log parameter in the init function.
  ####################################################################################################################
  def logMsg(self, msg ):
    if( self.log ):
      if(self.logger != None):
        self.logger.debug(msg)
      else:
        print( msg )
      
      

if __name__ == '__main__':
      
      
  parser = optparse.OptionParser()
  parser.add_option("-u", "--URL", dest="url",
                    help="The url to pull the file(s) from." )
  parser.add_option("-d", "--DestinationDir", dest="destDir",
                    help="The directory the remote files will be copied to locally." )
  parser.add_option("-m", "--FileMode", dest="fileMode",
                    help="The type of files being download, b=binary a=ascii." )
  parser.add_option("-f", "--UseFetchLog", dest="useFetchLog", action='store_true',
                    help="Specifies if a fetchlog is to be kept for each file download. This is used to prevent the copying of the same file over and over." )
  parser.add_option("-i", "--FetchLogDirectory", dest="fetchLogDir",
                    help="The directory to create the fetch logs." )
  parser.add_option("-l", "--LogMessages", dest="log", action= 'store_true',
                    help="If set, this will log out messages to StdOut" )
  parser.add_option("-r", "--regExpFilter", dest="filter", 
                    help="Regular Expression string used to further filter file results." )
  parser.add_option("-c", "--CheckFileModDate", dest="checkModDate", action= 'store_true',
                    help="If set, this will compare the remote files mod date against the fetch log files. Useful for when remote files always have the same name." )
  (options, args) = parser.parse_args()
  
  dlFiles = remoteFileDownload(options.url, 
                                options.destDir, 
                                options.fileMode, 
                                options.useFetchLog, 
                                options.fetchLogDir, 
                                options.log) 
  
  filter = options.filter
  if( filter == None ):
    filter = ""                              
  fileList = dlFiles.checkForNewFiles(filter)
  if( len(fileList) ):
    for file in fileList:
      rcvdFile = dlFiles.getFile(file,file,options.checkModDate)
           
          
