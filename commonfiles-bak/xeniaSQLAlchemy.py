"""
Revisions
Date: 2013-01-02
Function: xeniaAlchemy::obsTypeExists
Changes: Fixed up variable name in except handler
"""
import time


from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, Float, func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import eagerload
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy import exc
from sqlalchemy.orm.exc import *
#from geoalchemy import *
from geoalchemy2 import Geometry
import logging.config

Base = declarative_base()

class organization(Base):
  __tablename__ = 'organization'
  row_id           = Column(Integer,primary_key=True)                     
  row_entry_date   = Column(DateTime)       
  row_update_date  = Column(DateTime)       
  short_name       = Column(String(50))       
  active           = Column(Integer)                     
  long_name        = Column(String(200))      
  description      = Column(String(1000))    
  url              = Column(String(200))      
  opendap_url      = Column(String(200))      
  email_tech       = Column(String(150))                     

class collection_type(Base):
  __tablename__ = 'collection_type'
  row_id          = Column(Integer,primary_key=True)
  row_entry_date  = Column(DateTime)
  row_update_date = Column(DateTime)
  type_name       = Column(String)
  description     = Column(String)
  
class collection_run(Base):
  __tablename__ = 'collection'
  row_id           = Column(Integer,primary_key=True)
  row_entry_date   = Column(DateTime)
  row_update_date  = Column(DateTime)
  type_id          = Column(Integer, ForeignKey(collection_type.row_id))
  short_name       = Column(String)
  long_name        = Column(String)
  description      = Column(String)
  fixed_date       = Column(DateTime)
  min_date         = Column(DateTime)
  max_date         = Column(DateTime)
  fixed_lon        = Column(Float)
  min_lon          = Column(Float)
  max_lon          = Column(Float)
  fixed_lat        = Column(Float)
  min_lat          = Column(Float)
  max_lat          = Column(Float)
  fixed_z          = Column(Float)
  min_z            = Column(Float)
  max_z            = Column(Float)

class platform_type(Base):
  __tablename__ = 'platform_type'
  row_id        = Column(Integer, primary_key=True)                     
  type_name     = Column(String(50))      
  description    = Column(String(1000))
  short_name    = Column(String(50))

  def __init__(self, typeName, description, shortName):
    self.type_name = typeName
    self.description = description
    self.short_name  = shortName

class platform(Base):
  __tablename__ = 'platform'
  row_id           = Column(Integer,primary_key=True)                     
  row_entry_date   = Column(DateTime)       
  row_update_date  = Column(DateTime)       
  organization_id  = Column(Integer,ForeignKey(organization.row_id))                     
  type_id          = Column(Integer, ForeignKey(platform_type.row_id))                     
  short_name       = Column(String(50))       
  platform_handle  = Column(String(100))      
  fixed_longitude  = Column(Float)            
  fixed_latitude   = Column(Float)            
  active           = Column(Integer)                     
  begin_date       = Column(DateTime(timezone=False)) 
  end_date         = Column(DateTime(timezone=False)) 
  project_id       = Column(Integer)                     
  app_catalog_id   = Column(Integer)                     
  long_name        = Column(String(200))      
  description      = Column(String(1000))    
  url              = Column(String(200))      
  metadata_id      = Column(Integer)                     
  #the_geom         = GeometryColumn(Point(2))
  the_geom         = Column(Geometry('Point'))
  
  organization    = relationship(organization)   
  sensors         = relationship("sensor", order_by="sensor.row_id", backref="platform")
  platform_type   = relationship(platform_type)


        
class uom_type(Base):
  __tablename__ = 'uom_type'  
  row_id        = Column(Integer,primary_key=True)                     
  standard_name = Column(String(50))      
  definition    = Column(String(1000))      
  display       = Column(String(50))      
  
class obs_type(Base):
  __tablename__ = 'obs_type'  
  row_id        = Column(Integer,primary_key=True)                     
  standard_name = Column(String(50))      
  definition    = Column(String(1000))      

class m_scalar_type(Base):
  __tablename__ = 'm_scalar_type'  
  row_id        = Column(Integer,primary_key=True)                     
  obs_type_id   = Column(Integer,ForeignKey(obs_type.row_id))                     
  uom_type_id   = Column(Integer,ForeignKey(uom_type.row_id))            
  
  obs_type = relationship(obs_type)
  uom_type = relationship(uom_type)          

class m_type(Base):
  __tablename__ = 'm_type'  
  row_id           = Column(Integer,primary_key=True)                     
  num_types        = Column(Integer)
  description      = Column(String(1000))    
  m_scalar_type_id  = Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_2= Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_3= Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_4= Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_5= Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_6= Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_7= Column(Integer,ForeignKey(m_scalar_type.row_id))
  m_scalar_type_id_8= Column(Integer,ForeignKey(m_scalar_type.row_id))
  
  #We have to declare the primaryjoin statement since we have multiple ForeignKeys that point
  #to the same relationship.
  scalar_type = relationship(m_scalar_type, primaryjoin=(m_scalar_type_id==m_scalar_type.row_id))
  
class sensor(Base):
  __tablename__ = 'sensor'
  row_id           = Column(Integer,primary_key=True, autoincrement=True)                     
  row_entry_date   = Column(DateTime)       
  row_update_date  = Column(DateTime)       
  platform_id      = Column(Integer,ForeignKey(platform.row_id))                     
  type_id          = Column(Integer)                     
  short_name       = Column(String(50))       
  m_type_id        = Column(Integer,ForeignKey(m_type.row_id))
  fixed_z          = Column(Float)
  active           = Column(Integer)                     
  begin_date       = Column(DateTime(timezone=False)) 
  end_date         = Column(DateTime(timezone=False)) 
  s_order          = Column(Integer)
  url              = Column(String(200))      
  metadata_id      = Column(Integer)                     
  report_interval  = Column(Integer)                     
  
  #platform = relationship(platform, backref=backref("sensors"))
  m_type = relationship(m_type)
  
class multi_obs(Base):
  __tablename__ = 'multi_obs'  
  row_id           = Column(Integer,primary_key=True)                     
  row_entry_date   = Column(DateTime)    
  row_update_date  = Column(DateTime)    
  platform_handle  = Column(String(100))    
  sensor_id        = Column(Integer,ForeignKey(sensor.row_id))                     
  m_type_id        = Column(Integer,ForeignKey(m_type.row_id))                     
  m_date           = Column(DateTime(timezone=False)) 
  m_lon            = Column(Float)            
  m_lat            = Column(Float)             
  m_z              = Column(Float)             
  m_value          = Column(Float)             
  m_value_2        = Column(Float)             
  m_value_3        = Column(Float)             
  m_value_4        = Column(Float)             
  m_value_5        = Column(Float)             
  m_value_6        = Column(Float)             
  m_value_7        = Column(Float)             
  m_value_8        = Column(Float)             
  qc_metadata_id   = Column(Integer)                     
  qc_level         = Column(Integer)                     
  qc_flag          = Column(String(100))         
  qc_metadata_id_2 = Column(Integer)                     
  qc_level_2       = Column(Integer)                     
  qc_flag_2        = Column(String(100))         
  metadata_id      = Column(Integer)                     
  d_label_theta    = Column(Integer)                     
  d_top_of_hour    = Column(Integer)                     
  d_report_hour    = Column(DateTime(timezone=False)) 
  #the_geom         = GeometryColumn(Point(2))
  the_geom         = Column(Geometry('Point'))

  m_type          = relationship(m_type)
  sensor          = relationship(sensor)
  
  
  def __init__(self, 
              row_id           = None,
              row_entry_date   = None,
              row_update_date  = None,
              platform_handle  = None,
              sensor_id        = None,
              m_type_id        = None,
              m_date           = None,
              m_lon            = None,
              m_lat            = None,
              m_z              = None,
              m_value          = None,
              m_value_2        = None,
              m_value_3        = None,
              m_value_4        = None,
              m_value_5        = None,
              m_value_6        = None,
              m_value_7        = None,
              m_value_8        = None,
              qc_metadata_id   = None,
              qc_level         = None,
              qc_flag          = None,
              qc_metadata_id_2 = None,
              qc_level_2       = None,
              qc_flag_2        = None,
              metadata_id      = None,
              d_label_theta    = None,
              d_top_of_hour    = None,
              d_report_hour    = None
              ):
    self.row_id           = row_id          
    self.row_entry_date   = row_entry_date  
    self.row_update_date  = row_update_date 
    self.platform_handle  = platform_handle 
    self.sensor_id        = sensor_id       
    self.m_type_id        = m_type_id       
    self.m_date           = m_date          
    self.m_lon            = m_lon           
    self.m_lat            = m_lat           
    self.m_z              = m_z             
    self.m_value          = m_value         
    self.m_value_2        = m_value_2       
    self.m_value_3        = m_value_3       
    self.m_value_4        = m_value_4       
    self.m_value_5        = m_value_5       
    self.m_value_6        = m_value_6       
    self.m_value_7        = m_value_7       
    self.m_value_8        = m_value_8       
    self.qc_metadata_id   = qc_metadata_id  
    self.qc_level         = qc_level        
    self.qc_flag          = qc_flag         
    self.qc_metadata_id_2 = qc_metadata_id_2
    self.qc_level_2       = qc_level_2      
    self.qc_flag_2        = qc_flag_2       
    self.metadata_id      = metadata_id     
    self.d_label_theta    = d_label_theta   
    self.d_top_of_hour    = d_top_of_hour   
    self.d_report_hour    = d_report_hour   


class platform_status(Base):  
  __tablename__ = 'platform_status'
  row_id           = Column(Integer,primary_key=True)                     
  row_entry_date   = Column(DateTime(timezone=False))    
  begin_date       = Column(DateTime(timezone=False))    
  expected_end_date= Column(DateTime(timezone=False))    
  end_date         = Column(DateTime(timezone=False))    
  row_update_date  = Column(DateTime(timezone=False))    
  platform_handle  = Column(String(50))    
  author           = Column(String(100))    
  reason           = Column(String(500))    
  status           = Column(Integer)    
  platform_id      = Column(Integer, ForeignKey(platform.row_id))    
   
  platform = relationship(platform)

class sensor_status(Base):  
  __tablename__ = 'sensor_status'
  row_id           = Column(Integer,primary_key=True)
  sensor_id        = Column(Integer, ForeignKey(sensor.row_id))
  sensor_name      = Column(String(50))                     
  platform_id      = Column(Integer, ForeignKey(platform.row_id))    
  row_entry_date   = Column(DateTime(timezone=False))    
  begin_date       = Column(DateTime(timezone=False))    
  end_date         = Column(DateTime(timezone=False))    
  expected_end_date= Column(DateTime(timezone=False))    
  row_update_date  = Column(DateTime(timezone=False))    
  author           = Column(String(100))    
  reason           = Column(String(500))    
  status           = Column(Integer)    
 
  platform = relationship(platform)
  sensor   = relationship(sensor)

class product_type(Base):
  __tablename__     = 'product_type'
  row_id            = Column(Integer,primary_key=True)
  type_name         = Column(String(50))
  description       = Column(String(1000))

class timestamp_lkp(Base):
  __tablename__     = 'timestamp_lkp'
  row_id            = Column(Integer,primary_key=True)
  row_entry_date    = Column(DateTime(timezone=False))
  row_update_date   = Column(DateTime(timezone=False))
  product_id        = Column(Integer,ForeignKey(product_type.row_id))
  pass_timestamp    = Column(DateTime(timezone=False))
  filepath          = Column(String(200))

class xeniaAlchemy(object):
  def __init__(self, logger=True):
    self.dbEngine = None
    self.metadata = None
    self.session  = None
    self.logger   = logger
    if logger:
      self.logger = logging.getLogger(__name__)
    
  def connectDB(self, databaseType, dbUser, dbPwd, dbHost, dbName, printSQL = False):
    
    try:
      #Connect to the database
      if(dbHost != None and len(dbHost)):
        connectionString = "%s://%s:%s@%s/%s" %(databaseType, dbUser, dbPwd, dbHost, dbName)
      else:
        connectionString = "%s://%s:%s@/%s" %(databaseType, dbUser, dbPwd, dbName)
         
      self.dbEngine = create_engine(connectionString, echo=printSQL)
      
      #metadata object is used to keep information such as datatypes for our table's columns.
      self.metadata = MetaData()
      self.metadata.bind = self.dbEngine
      
      Session = sessionmaker(bind=self.dbEngine)  
      self.session = Session()
      
      self.connection = self.dbEngine.connect()
      
      return(True)
    except exc.OperationalError, e:
      if(self.logger != None):
        self.logger.exception(e)
    return(False)

  def disconnect(self):
    self.session.close()
    self.connection.close()
    self.dbEngine.dispose()
  
  """
  Function: platformExists  
  """
  def platformExists(self, platformHandle):
    try:
      platRec = self.session.query(platform.row_id)\
        .filter(platform.platform_handle == platformHandle)\
        .one()
      return(platRec.row_id)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)    
    return(None)
    
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
  def newPlatform(self, rowEntryDate, platformHandle, fixedLongitude, fixedLatitude, active=1, url="", description=""):
    platformRec = None
    platformHandleParts = platformHandle.split('.')    
    #Check to make sure the organization exists:
    orgId = self.organizationExists(platformHandleParts[0])
    if(orgId == None):
      if(self.logger):
        self.logger.debug("Organization: %s does not exist. Adding." % (platformHandleParts[0]))
      orgId = self.addOrganization(rowEntryDate, platformHandleParts[0])
      if(orgId == None):
        if(self.logger):
          self.logger.error("Could not add organization, cannot continue adding platform.")
          return(None)
    """    
    #Get platform type id.
    platTypeId = self.platformTypeExists(platformHandleParts[2])
    if(platTypeId == None):
      if(self.logger):
        self.logger.error("Platform type: %s does not exist." % (platformHandleParts[2]))
    """
    try:
      platformRec = platform(row_entry_date = rowEntryDate,
                             organization_id = orgId,
                              short_name = platformHandleParts[1],        
                              platform_handle = platformHandle,
                              fixed_longitude = fixedLongitude,             
                              fixed_latitude = fixedLatitude,             
                              active = active,   
                              url = url,
                              description = description)                   
            
      platformRecId = self.addRec(platformRec, True)
      if(self.logger):
        self.logger.debug("Platform: %s(%d) added to database." % (platformRec.platform_handle, platformRec.row_id))
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
        
    return(platformRec.row_id)
  """
  Function: addOrganization
  """
  def addOrganization(self, rowEntryDate, organizationName, active=1, longName="", description="", url=""):
    orgRec = organization(row_entry_date=rowEntryDate,
                          short_name=organizationName,
                          active=active,
                          long_name=longName,
                          description=description,
                          url=url)    
    rowId = self.addRec(orgRec, True)
    return(rowId)
  
  """
  Function: organizationExists
  """
  def organizationExists(self, organizationName):
    try:
      orgRec = self.session.query(organization.row_id)\
        .filter(organization.short_name == organizationName)\
        .one()
      return(orgRec.row_id)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)    
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
  def sensorExists(self, obsName, uom, platformHandle, sOrder=1 ):
    
    try:  
      rec = self.session.query(sensor.row_id)\
        .join((platform, platform.row_id == sensor.platform_id))\
        .join((m_type, m_type.row_id == sensor.m_type_id))\
        .join((m_scalar_type, m_scalar_type.row_id == m_type.m_scalar_type_id))\
        .join((obs_type, obs_type.row_id == m_scalar_type.obs_type_id))\
        .join((uom_type, uom_type.row_id == m_scalar_type.uom_type_id))\
        .filter(sensor.s_order == sOrder)\
        .filter(platform.platform_handle == platformHandle)\
        .filter(obs_type.standard_name == obsName)\
        .filter(uom_type.standard_name == uom).one()
      return(rec.row_id)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    
    return(None)
  
  def newSensor(self, rowEntryDate, obsName, uom, platformId, active=1, fixedZ=0, sOrder=1, mTypeId=None, addObsAndUOM=False):
    if(self.logger):
      self.logger.debug("Adding sensor: %s(%s) sOrder: %d on platform: %d" % (obsName, uom, sOrder, platformId))
          
    if(mTypeId == None):
      mTypeId = self.mTypeExists(obsName, uom)
      #TODO: Add all the surrounding uom, and obs entries, m_type and scalar.
      if(mTypeId == None):
        #If we want to add the obs type and uom type, we have to add them to add to tables: obs_type, uom_type, m_scalar_type
        #before we can add the m_type.
        if(addObsAndUOM):
          #Does obs_type exist? If not, we attempt to add.
          obsId = self.obsTypeExists(obsName)
          if(obsId == -1):
            #Add the obs to the obs_type table.
            obsId = self.addObsType(obsName)
            #Cannot continue if we were unable to add.
            if(obsId == None):
              return(None)
            
          #Does the uom type exist? If not, we attempt to add.
          uomId = self.uomTypeExists(uom)
          if(uomId == -1):            
            uomId = self.addUOMType(uom)
            #Cannot continue if we were unable to add.
            if(uomId == None):
              return(None)
            
          #Does the scalar_id exist?
          mScalarId = self.scalarTypeExists(obsId, uomId)  
          if(mScalarId == -1):
            mScalarId = self.addScalarType(obsId, uomId)
            if(mScalarId == None):
              return(None)
          
          #Now we can add the m_type
          mTypeId = self.addMType(mScalarId)
          if(mTypeId == None):
            return(None)
        else:
          if(self.logger):
            self.logger.error("m_type does not exist, cannot add sensor: %s(%s) platform: %d" % (obsName, uom, platformId))
            return(None)

      sensorRec = sensor(row_entry_date = rowEntryDate,
                         platform_id = platformId,
                         m_type_id = mTypeId,
                         short_name = obsName,
                         fixed_z = fixedZ,
                         active = active,
                         s_order = sOrder)
      sensorId = self.addRec(sensorRec, True)
      if(sensorId == None):
        if(self.logger):
          self.logger.error("Unable to add sensor: %s(%s)." % (obsName,uom))
      else:
        if(self.logger):
          self.logger.debug("Added sensor: %s(%s) sOrder: %d on platform: %d" % (obsName, uom, sOrder, platformId))
    return(sensorId)

  """
  Function: mTypeExists
  Purpose: Checks to see if the passed in obsName with the given units of measurement exists in the m_type table.
  Parameters: 
    obsName is the sensor(observation) we are testing for.
  Returns:
    The m_type id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def mTypeExists(self, obsName, uom):
    try:  
      rec = self.session.query(m_type.row_id)\
        .join((m_scalar_type, m_scalar_type.row_id == m_type.m_scalar_type_id))\
        .join((obs_type, obs_type.row_id == m_scalar_type.obs_type_id))\
        .join((uom_type, uom_type.row_id == m_scalar_type.uom_type_id))\
        .filter(obs_type.standard_name == obsName)\
        .filter(uom_type.standard_name == uom).one()
      return(rec.row_id)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug("m_type %s(%s) does not exist." % (obsName, uom))
        #self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    
    return(None)

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
  def addMType(self, scalarID, description=""):
    rowId = None    
    #At the moment the row_id columns are not autoincrement, so we need to get the max value first.
    try:
      nextRowId = self.session.query(func.max(m_type.row_id)).one()[0]
      nextRowId += 1
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      mTypeRec = m_type(row_id=nextRowId, num_types=1, m_scalar_type_id=scalarID, description=description)
      rowId = self.addRec(mTypeRec, True )
      if(rowId == None):
        if(self.logger):
          self.logger.error("Unable to add scalarID: %d to m_type table." % (scalarID))
      else:
        if(self.logger):
          self.logger.debug("Added scalarID: %d to m_type table." % (scalarID)) 
    return(rowId)

  
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
    rowId = None
    try:
      rec = self.session.query(obs_type.row_id)\
        .filter(obs_type.standard_name == obsName)\
        .one()
      rowId = rec.row_id
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug("Observation: %s does not exist in obs_type table." % (obsName))
      rowId = -1
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
        
    return(rowId)
  """
  Function: addObsType
  Purpose: Adds the given obsName into the obs_type table.
    obsName is the sensor(observation) we are adding.
  Returns:
    The obs_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addObsType(self, obsName):
    rowId = None
    #At the moment the row_id columns are not autoincrement, so we need to get the max value first.
    try:
      nextRowId = self.session.query(func.max(obs_type.row_id)).one()[0]
      nextRowId += 1
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      obsTypeRec = obs_type(row_id=nextRowId, standard_name=obsName)
      rowId = self.addRec(obsTypeRec, True)
      if(rowId == None):
        if(self.logger):
          self.logger.error("Unable to add obs: %s to obs_type table." % (obsName))
      else:
        if(self.logger):
          self.logger.debug("Added obs: %s to obs_type table." % (obsName)) 
    return(rowId)
  
  """
  Function: uomTypeExists
  Purpose: Checks to see if the passed in uomName exists in the uom_type table.
  Parameters: 
    uomName is the unit of measurement  we are testing for.
  Returns:
    The uom_type(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def uomTypeExists(self, uomName):
    rowId = None
    try:
      rec = self.session.query(uom_type.row_id)\
        .filter(uom_type.standard_name == uomName)\
        .one()
      rowId = rec.row_id
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug("UOM: %s does not exist in obs_type table." % (uomName))
      rowId = -1  
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    return(rowId)
  
  """
  Function: addUOMType
  Purpose: Adds the given obsName into the obs_type table.
    obsName is the sensor(observation) we are adding.
  Returns:
    The uom_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
    lastErrorMsg can be checked for the error message.
  """
  def addUOMType(self, uomName):
    rowId = None
    #At the moment the row_id columns are not autoincrement, so we need to get the max value first.
    try:
      nextRowId = self.session.query(func.max(uom_type.row_id)).one()[0]
      nextRowId += 1
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      uomTypeRec = uom_type(row_id=nextRowId, standard_name=uomName)
      rowId = self.addRec(uomTypeRec, True)
      if(rowId == None):
        if(self.logger):
          self.logger.error("Unable to add uom: %s to uom_type table." % (uomName))
      else:
        if(self.logger):
          self.logger.debug("Added uom: %s to obs_type table." % (uomName)) 
    return(rowId)

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
    rowId = None
    try:
      rec = self.session.query(m_scalar_type.row_id)\
        .filter(m_scalar_type.obs_type_id == obsTypeID)\
        .filter(m_scalar_type.uom_type_id == uomTypeID)\
        .one()
      rowId = rec.row_id
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug("Scalar type for obs_type_id: %d uom_type_id: %d does not exist in m_scalar_type table." %(obsTypeID, uomTypeID))
      rowId = -1  
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    return(rowId)
  
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
    rowId = None
    #At the moment the row_id columns are not autoincrement, so we need to get the max value first.
    try:
      nextRowId = self.session.query(func.max(m_scalar_type.row_id)).one()[0]
      nextRowId += 1
    except Exception,e:
      if(self.logger):
        self.logger.exception(e)
    else:
      scalarRec = m_scalar_type(row_id=nextRowId, obs_type_id=obsTypeID, uom_type_id=uomTypeID)
      rowId = self.addRec(scalarRec, True)
      if(rowId == None):
        if(self.logger):
          self.logger.error("Unable to add m_scalar_type: obs_type_id: %d  uom_type_id: %d to m_scalar_type table." % (obsTypeID, uomTypeID))
      else:
        if(self.logger):
          self.logger.debug("Added m_scalar_type: obs_type_id: %d  uom_type_id: %d to m_scalar_type table." % (obsTypeID, uomTypeID))
    return(rowId)



  def getCurrentPlatformStatus(self, platformHandle):
    try:
      rec = self.session.query(sensor_status.status)\
          .filter(platform_status.platform_handle==platformHandle).one()
      return(rec.status)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    return(None)  

  def getCurrentSensorStatus(self, obsName, platformHandle):
    try:
      rec = self.session.query(platform_status.status)\
          .filter(platform.platform_handle==platformHandle)\
          .filter(sensor_status.sensor_name==obsName).one()
      return(rec.status)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    return(None)  
  
  def platformTypeExists(self, platformType):
    try:
      platRec = self.session.query(platform_type.row_id)\
        .filter(platform_type.type_name == platformType)\
        .one()
      return(platRec.row_id)
    except NoResultFound, e:
      if(self.logger != None):
        self.logger.debug(e)
    except exc.InvalidRequestError, e:
      if(self.logger != None):
        self.logger.exception(e)
    return(None)
  
  def addPlatformType(self, typeName, description="", commit=False):
    platType= None
    try:
      platType = platform_type(typeName, description)
      self.session.add(platType)
      if(commit):
        self.session.commit()
    #Trying to add record that already exists.
    except exc.IntegrityError, e:
      self.session.rollback()
      if(self.logger != None):
        self.logger.exception(e)
    return(platType)
  
  def addRec(self, rec, commit=False):
    try:
      self.session.add(rec)
      if(commit):
        self.session.commit()
    #Trying to add record that already exists.
    except exc.IntegrityError, e:
      self.session.rollback()
      if(self.logger != None):
        self.logger.exception(e)
    return(rec.row_id)
    
  
  def addPlatform(self, platformRec, commit=False):  
    return(self.addRec(platformRec, commit))

  def addSensor(self, sensorRec, commit=False):
    return(self.addRec(sensorRec, commit))
    
if __name__ == '__main__':
  xeniaDB = xeniaAlchemy()
