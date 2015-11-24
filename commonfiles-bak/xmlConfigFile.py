"""
Revisions
Date: 9/22/2011
Author: DWR
Function: xmlConfigFile.getEmailSettings
Changes: Calls xmlConfigFile.getEmailSettingsEx

Function: xmlConfigFile.getEmailSettingsEx
Changes: toList is now an array instead of a comma string.

Function: xmlConfigFile.getDatabaseSettings
Changes: Calls getDatabaseSettingsEx now
"""
from lxml import etree



'''
Class: xmlConfigFile
Purpose: Base class for working with "standard" xml configuration files.
'''

class xmlConfigFile(object):
  def __init__(self, xmlConfigFileName, parseFile=True):
    self.xmlConfigFileName = xmlConfigFileName
    #We want to go ahead and parse the file in memory.
    if( parseFile ):
      self.xmlTree = etree.parse(self.xmlConfigFileName)
  '''
  Function: getEntry
  Purpose: Attemps to look up a xml tag hierarchy and returns the associated text if found.
  Parameters:
    xmlTag is the xpath tag to look for. In the form of \\root\child\node
    xmlElement is the Element object to work on. If not provided, the default will be whatever self.xmlTree 
      currently points at.
  Returns: 
    The text associated with the path, or None if not found.
  '''
  def getEntry(self, xmlTag, xmlElement=None ):
    if( xmlElement == None ):
      xmlElement = self.xmlTree  
    tag = xmlElement.xpath( xmlTag )
    if( len(tag) ):
      return( tag[0].text )
    return(None)
  
  '''
  Function: getListHead
  Purpose: For a section that is a list for a particular set of parameters, this returns the
    Element which will be the head of the list. To iterate over this list, call xmlConfigFile.getNextInList()
    The structure of a list would look similar to this:
    <list>
      <item1>
        <entryA> text </entryA>
        <entryB> text </entryB>
        <entryC> text </entryC>
      </item1>
      <item2>
        <entryA> text </entryA>
        <entryB> text </entryB>
        <entryC> text </entryC>
      </item2>
    </list>
  Parameters:
    xmlTag is the xpath to search for.
  Returns:
    If xmlTag found, returns an Element for iteration on, otherwise None.
  '''
  def getListHead(self, xmlTag, xmlElement=None):
    if(xmlElement == None):
      return( self.xmlTree.xpath(xmlTag) )
    else:
      return(xmlElement.xpath(xmlTag))
  
  def getNextInList(self, elementList ):
    return( elementList[0].getchildren() )
  '''
  Function: getDatabaseSettings
  Purpose: Returns various settings from the database section of the config file.
  The structure for this section is:
    <environment>
      <database>
        <db>
          <type></type>
          <name></name>
          <user></user>
          <pwd></pwd>
          <host></host>
        </db>
      </database> 
   Parameters: None
   Return: Returns a keyed dictionary which could have the following keys.
     For sqlite:
       ['type'] is the type of the database, will contain 'sqlite'.
       ['dbName'] is the full path to the sqlite db file.
    For postgres
       ['type'] is the type of the database, will contain 'postgres'.
       ['dbName'] is the postgres database name.
       ['dbUser'] is the user to logon to the postgres server with.
       ['dbPwd'] is the password used for the dbUser above.
       ['dbHost'] is the IP address of the host machine running the postgres instance.
  '''
  def getDatabaseSettings(self):
    """
    settings = None    
    type = self.getEntry( '//environment/database/db/type' )
    if( type != None ):
      if( type == 'sqlite' ):
        xmlVal = self.getEntry( '//environment/database/db/name' )
        if( xmlVal != None ):
          settings = {}
          settings['type']  = 'sqlite'
          settings['dbName'] = xmlVal
        else:
          return(None)
      elif( type == 'postgres' ):
        settings = {}
        settings['type']   = 'postgres'
        settings['dbName'] = self.getEntry( '//environment/database/db/name' )
        settings['dbUser'] = self.getEntry( '//environment/database/db/user' )
        settings['dbPwd']  = self.getEntry( '//environment/database/db/pwd' )
        settings['dbHost'] = self.getEntry( '//environment/database/db/host' )
    """   
    return(self.getDatabaseSettingsEx())

  def getDatabaseSettingsEx(self, tagPrefix='//environment/database/db/' ):
    settings = None    
    type = self.getEntry( tagPrefix + 'type' )
    if( type != None ):
      if( type == 'sqlite' ):
        xmlVal = self.getEntry( tagPrefix + 'name' )
        if( xmlVal != None ):
          settings = {}
          settings['type']  = 'sqlite'
          settings['dbName'] = xmlVal
        else:
          return(None)
      elif( type == 'postgres' ):
        settings = {}
        settings['type']   = 'postgres'
        settings['dbName'] = self.getEntry( tagPrefix + 'name' )
        settings['dbUser'] = self.getEntry( tagPrefix + 'user' )
        settings['dbPwd']  = self.getEntry( tagPrefix + 'pwd' )
        settings['dbHost'] = self.getEntry( tagPrefix + 'host' )
       
    return( settings )
  
  '''
  Function: getEmailSettings
  Purpose: Returns various settings from the email section of the config file.
  The structure for this section is:
    <environment>
      <emailSettings>
        <server></server>
        <from></from>
        <pwd></pwd>
        <emailList>
          <email>...</<email>
          <email>...</<email>
        </emailList>
      </database> 
   Parameters: None
   Return: Returns a keyed dictionary which could have the following keys.
       ['server'] is smpt server to send the email through.
       ['from'] is user the email is to be addressed from 
       ['pwd'] is the user to logon to the smtp server with.
       ['toList'] comma delimited list of the recipients
  '''
  def getEmailSettings(self):
    """
    settings = {}
    settings['server'] =  self.getEntry( '//environment/emailSettings/server' )
    settings['from'] = self.getEntry( '//environment/emailSettings/from' )
    settings['pwd']  = self.getEntry( '//environment/emailSettings/pwd' )
    settings['toList'] = []
    emailAddys = ''
    recptList = self.getListHead("//environment/emailSettings/emailList")
    for child in self.getNextInList(recptList):
      settings['toList'].append(child.text)
    """
    return(self.getEmailSettingsEx())

  def getEmailSettingsEx(self, tagPrefix='//environment/emailSettings/'):
    settings = {}
    settings['server'] =  self.getEntry(tagPrefix + "/server")
    settings['from'] = self.getEntry(tagPrefix + "/from")
    settings['pwd']  = self.getEntry(tagPrefix + "/pwd")
    settings['toList'] = []
    emailAddys = ''
    recptList = self.getListHead(tagPrefix + "/emailList")
    for child in self.getNextInList(recptList):
      settings['toList'].append(child.text)
    return( settings )
