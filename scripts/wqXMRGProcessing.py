import sys
sys.path.append('../commonfiles')

import os

import logging.config
import optparse
import ConfigParser
import time
import re
from datetime import datetime, timedelta
from pytz import timezone
import requests
from multiprocessing import Process, Queue, current_process

from shapely.geometry import Polygon
from shapely.wkt import loads as wkt_loads

from pykml.factory import KML_ElementMaker as KML
from lxml import etree
from wqDatabase import wqDB

#from processXMRGFile import processXMRGData
from wqHistoricalData import item_geometry, geometry_list
from xmrgFile import xmrgFile, hrapCoord, LatLong, nexrad_db, getCollectionDateFromFilename

"""
class configSettings(object):
  def __init__(self, config_file):
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(config_file)

      bbox = configFile.get('nexrad_database', 'bbox')
      self.minLL = None
      self.maxLL = None
      if(bbox != None):
        latLongs = bbox.split(';')
        self.minLL = LatLong()
        self.maxLL = LatLong()
        latlon = latLongs[0].split(',')
        self.minLL.latitude = float( latlon[0] )
        self.minLL.longitude = float( latlon[1] )
        latlon = latLongs[1].split(',')
        self.maxLL.latitude = float( latlon[0] )
        self.maxLL.longitude = float( latlon[1] )

      #Delete data that is older than the LastNDays
      self.xmrgKeepLastNDays = configFile.getint('nexrad_database', 'keepLastNDays')

      #Try to fill in any holes in the data going back N days.
      self.backfillLastNDays = configFile.getint('nexrad_database', 'backfillLastNDays')

      #Flag to specify whether or not to write the precip data to the database.
      self.writePrecipToDB = configFile.getboolean('nexrad_database', 'writeToDB')

      self.writePrecipToKML = configFile.getboolean('nexrad_database', 'writeToKML')

      #If we are going to write shapefiles, get the output directory.
      if(self.writePrecipToKML):
        self.KMLDir = configFile.get('nexrad_database', 'KMLDir')
        if(len(self.KMLDir) == 0):
          self.writePrecipToKML = 0
          if self.logger is not None:
            self.logger.error("No KML directory provided, will not write shapefiles.")

      self.saveAllPrecipVals = configFile.getboolean('nexrad_database', 'saveAllPrecipVals')

      self.createPolygonsFromGrid = configFile.getboolean('nexrad_database', 'createPolygonsFromGrid')

      #Flag to specify if we want to delete the compressed XMRG file when we are done processing.
      #We might not be working off a compressed source file, so this flag only applies to a compressed file.
      self.deleteCompressedSourceFile = configFile.getboolean('nexrad_database', 'deleteCompressedSourceFile')

      #Flag to specify if we want to delete the XMRG file when we are done processing.
      self.deleteSourceFile = configFile.getboolean('nexrad_database', 'deleteSourceFile')

      #Directory to import XMRG files from
      self.importDirectory = configFile.get('nexrad_database', 'importDirectory')

      #Flag to specify if we want to calculate the weighted averages for the watersheds as we write the radar data
      #into the precipitation_radar table.
      self.calcWeightedAvg =configFile.getboolean('nexrad_database', 'calculateWeightedAverage')


      self.dbName = configFile.get('database', 'name')
      self.spatiaLiteLib = configFile.get('database', 'spatiaLiteLib')


      self.baseURL = configFile.get('nexrad_database', 'baseURL')
      #This tag is used to help further refine the files we process. For instance, hourly xmrg files are prepended
      #with xmrg whereas the 6hr and 24hr files aren't. So we could use this to ignore those.
      self.fileNameFilter = configFile.get('nexrad_database', 'fileNameFilter')
      self.xmrgDLDir = configFile.get('nexrad_database', 'downloadDir')

      #Directory where the NEXRAD database schema files live.
      self.nexrad_schema_directory = configFile.get('nexrad_database', 'schema_directory')
      #The files that create the tables we need in our NEXRAD DB.
      self.nexrad_schema_files = configFile.get('nexrad_database', 'schema_files').split(',')

      #File containing the boundaries we want to use to carve out data from the XMRG file.
      self.boundaries_file = configFile.get('boundaries_settings', 'boundaries_file')

      #Number of worker processes to start.
      self.worker_process_count = configFile.getint('nexrad_database', 'worker_process_count')

      #Specifies to attempt to add the sensors before inserting the data. Only need to do this
      #on intial run.
      self.add_sensors = True
      #Specifies to attempt to add the platforms representing the radar coverage.
      self.add_platforms = True

      self.save_boundary_grid_cells = True
      self.save_boundary_grids_one_pass = True

    except (ConfigParser.Error, Exception):
      pass
"""
class xmrg_results(object):
  def __init__(self):
    self.datetime = None
    self.boundary_results = {}
    self.boundary_grids = {}

  def add_boundary_result(self, name, result_type, result_value):
    if name not in self.boundary_results:
      self.boundary_results[name] = {}

    results = self.boundary_results[name]
    results[result_type] = result_value

  def get_boundary_results(self, name):
    return(self.boundary_results[name])

  def add_grid(self, boundary_name, grid_tuple):

    if boundary_name not in self.boundary_grids:
      self.boundary_grids[boundary_name] = []

    grid_data = self.boundary_grids[boundary_name]
    grid_data.append(grid_tuple)

  def get_boundary_grid(self, boundary_name):
    grid_data = None
    if boundary_name in self.boundary_grids:
      grid_data = self.boundary_grids[boundary_name]
    return grid_data

  def get_boundary_data(self):
    for boundary_name, boundary_data in self.boundary_results.iteritems():
      yield (boundary_name, boundary_data)

def process_xmrg_file(**kwargs):
  try:
    processing_start_time = time.time()
    xmrg_file_count = 1
    logger = None
    if 'logger' in kwargs:
      if kwargs['logger']:
        logger = logging.getLogger(current_process().name)
        logger.debug("%s starting process_xmrg_file." % (current_process().name))

    inputQueue = kwargs['input_queue']
    resultsQueue = kwargs['results_queue']
    save_all_precip_vals = kwargs['save_all_precip_vals']
    #A course bounding box that restricts us to our area of interest.
    minLatLong = None
    maxLatLong = None
    if 'min_lat_lon' in kwargs and 'max_lat_lon' in kwargs:
      minLatLong = kwargs['min_lat_lon']
      maxLatLong = kwargs['max_lat_lon']

    #Boundaries we are creating the weighted averages for.
    boundaries = kwargs['boundaries']

    save_boundary_grid_cells = True
    save_boundary_grids_one_pass = True

    #This is the database insert datetime.
    datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )

    #Create the precip database we use local to the thread.
    #nexrad_filename = "%s%s.sqlite" % (kwargs['nexrad_schema_directory'], current_process().name)
    #if os.path.isfile(nexrad_filename):
    #  os.remove(nexrad_filename)
    nexrad_db_conn = nexrad_db()
    nexrad_db_conn.connect(db_name=":memory:",
                           spatialite_lib=kwargs['spatialite_lib'],
                           nexrad_schema_files=kwargs['nexrad_schema_files'],
                           nexrad_schema_directory=kwargs['nexrad_schema_directory']
                           )
    #nexrad_db_conn.db_connection.isolation_level = None
    nexrad_db_conn.db_connection.execute("PRAGMA synchronous = OFF")
    nexrad_db_conn.db_connection.execute("PRAGMA journal_mode = MEMORY")
  except Exception,e:
    if logger:
      logger.exception(e)

  else:
    for xmrg_filename in iter(inputQueue.get, 'STOP'):
      tot_file_time_start = time.time()
      if logger:
        logger.debug("ID: %s processing file: %s" % (current_process().name, xmrg_filename))

      xmrg_proc_obj = wqXMRGProcessing(logger=False)
      xmrg = xmrgFile(current_process().name)
      xmrg.openFile(xmrg_filename)

      #Data store in hundreths of mm, we want mm, so convert.
      dataConvert = 100.0

      if xmrg.readFileHeader():
        if logger:
          logger.debug("ID: %s File Origin: X %d Y: %d Columns: %d Rows: %d" %(current_process().name, xmrg.XOR,xmrg.YOR,xmrg.MAXX,xmrg.MAXY))
        try:
          read_rows_start = time.time()
          if xmrg.readAllRows():
            if logger:
              logger.debug("ID: %s(%f secs) to read all rows in file: %s" % (current_process().name, time.time() - read_rows_start, xmrg_filename))
            #This is the database insert datetime.
            #Parse the filename to get the data time.
            (directory, filetime) = os.path.split(xmrg.fileName)
            (filetime, ext) = os.path.splitext(filetime)
            filetime = xmrg_proc_obj.getCollectionDateFromFilename(filetime)

            #Flag to specifiy if any non 0 values were found. No need processing the weighted averages
            #below if nothing found.
            rainDataFound=False
            #If we are using a bounding box, let's get the row/col in hrap coords.
            llHrap = None
            urHrap = None
            start_col = 0
            start_row = 0
            end_col = xmrg.MAXX
            end_row = xmrg.MAXY
            if minLatLong != None and maxLatLong != None:
              llHrap = xmrg.latLongToHRAP(minLatLong, True, True)
              urHrap = xmrg.latLongToHRAP(maxLatLong, True, True)
              start_row = llHrap.row
              start_col = llHrap.column
              end_row = urHrap.row
              end_col = urHrap.column

            recsAdded = 0
            results = xmrg_results()

            #trans_cursor = nexrad_db_conn.db_connection.cursor()
            #trans_cursor.execute("BEGIN")
            add_db_rec_total_time = 0
            #for row in range(startRow,xmrg.MAXY):
            #  for col in range(startCol,xmrg.MAXX):
            for row in range(start_row, end_row):
              for col in range(start_col, end_col):
                hrap = hrapCoord(xmrg.XOR + col, xmrg.YOR + row)
                latlon = xmrg.hrapCoordToLatLong(hrap)
                latlon.longitude *= -1
                val = xmrg.grid[row][col]
                #If there is no precipitation value, or the value is erroneous
                if val <= 0:
                  if save_all_precip_vals:
                    val = 0
                  else:
                    continue
                else:
                  val /= dataConvert

                rainDataFound = True
                #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
                #that has each point in the grid for a given point.
                hrapNewPt = hrapCoord( xmrg.XOR + col, xmrg.YOR + row + 1)
                latlonUL = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonUL.longitude *= -1

                hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row)
                latlonBR = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonBR.longitude *= -1

                hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row + 1)
                latlonUR = xmrg.hrapCoordToLatLong( hrapNewPt )
                latlonUR.longitude *= -1

                grid_polygon = Polygon([(latlon.longitude, latlon.latitude),
                                        (latlonUL.longitude, latlonUL.latitude),
                                        (latlonUR.longitude, latlonUR.latitude),
                                        (latlonBR.longitude, latlonBR.latitude),
                                        (latlon.longitude, latlon.latitude)])
                if save_boundary_grid_cells:
                  results.add_grid('complete_area', (grid_polygon, val))

                try:
                  add_db_rec_start = time.time()
                  nexrad_db_conn.insert_precip_record(datetime, filetime,
                                                      latlon.latitude, latlon.longitude,
                                                      val,
                                                      grid_polygon,
                                                      None)
                  #if logger:
                  # logger.debug("ID: %s(%f secs insert)" % (current_process().name, time.time() - add_db_rec_start))
                  add_db_rec_total_time += time.time() - add_db_rec_start

                  recsAdded += 1
                except Exception,e:
                  if logger:
                    logger.exception(e)
                  nexrad_db_conn.db_connection.rollback()
            #Commit the inserts.
            try:
              commit_recs_start = time.time()
              nexrad_db_conn.commit()
              commit_recs_time = time.time() - commit_recs_start
            except Exception,e:
              if logger:
                logger.exception(e)
                nexrad_db.db_connection.rollback()
            else:
              if logger is not None:
                logger.info("ID: %s(%f secs add %f secs commit) Processed: %d rows. Added: %d records to database."\
                            %(current_process().name, add_db_rec_total_time, commit_recs_time, (row + 1),recsAdded))

              results.datetime = filetime
              for boundary in boundaries:
                try:
                  if save_boundary_grid_cells:
                    boundary_grid_query_start = time.time()
                    #cells_cursor = nexrad_db_conn.get_radar_data_for_boundary(boundary['polygon'], filetime, filetime)
                    cells_cursor = nexrad_db_conn.get_radar_data_for_boundary(boundary.object_geometry, filetime, filetime)
                    for row in cells_cursor:
                      cell_poly = wkt_loads(row['WKT'])
                      precip = row['precipitation']
                      #results.add_grid(boundary['name'], (cell_poly, precip))
                      results.add_grid(boundary.name, (cell_poly, precip))

                    if logger:
                      logger.debug("ID: %s(%f secs) to query grids for boundary: %s"\
                                   % (current_process().name, time.time() - boundary_grid_query_start, boundary.name))


                  avg_start_time = time.time()
                  #avg = nexrad_db_conn.calculate_weighted_average(boundary['polygon'], filetime, filetime)
                  avg = nexrad_db_conn.calculate_weighted_average(boundary.object_geometry, filetime, filetime)
                  #results.add_boundary_result(boundary['name'], 'weighted_average', avg)
                  results.add_boundary_result(boundary.name, 'weighted_average', avg)
                  avg_total_time = time.time() - avg_start_time
                  if logger:
                    logger.debug("ID: %s(%f secs) to process average for boundary: %s"\
                                 % (current_process().name, avg_total_time, boundary.name))
                except Exception,e:
                  if logger:
                    logger.exception(e)
            resultsQueue.put(results)

            nexrad_db_conn.delete_all()
          #Only do it for one file. Following files should all be same results other than the precip values.
          if save_boundary_grids_one_pass:
            save_boundary_grid_cells = False
          xmrg.cleanUp(kwargs['delete_source_file'], kwargs['delete_compressed_source_file'])
          xmrg.Reset()
          #Counter for number of files processed.
          xmrg_file_count += 1
          if logger:
            logger.debug("ID: %s(%f secs) total time to process data for file: %s" % (current_process().name, time.time() - tot_file_time_start, xmrg_filename))
        except Exception,e:
          if logger:
            logger.exception(e)



    if nexrad_db_conn:
      nexrad_db_conn.close()

    if logger:
      logger.debug("ID: %s process finished. Processed: %d files in time: %f seconds"\
                   % (current_process().name, xmrg_file_count, time.time() - processing_start_time))
    return

"""
Want to move away form the XML config file used and use an ini file. Create a new class
inheritting from the dhec one.
"""
class wqXMRGProcessing(object):
  def __init__(self, logger=True):

    self.logger = None
    if logger:
      self.logger = logging.getLogger(type(self).__name__)
      self.xenia_db = None
      self.boundaries = geometry_list(use_logger=True) #[]
      self.sensor_ids = {}
    try:
      #2011-07-25 DWR
      #Added a processing start time to use for the row_entry_date value when we add new records to the database.
      self.processingStartTime = time.strftime('%Y-%d-%m %H:%M:%S', time.localtime())
      self.configSettings = None

    except (ConfigParser.Error, Exception) as e:
      if self.logger is not None:
        self.logger.exception(e)
      pass

  def load_config_settings(self, **kwargs):
    #self.configSettings = configSettings(kwargs['config_file'])
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(kwargs['config_file'])

      bbox = configFile.get('nexrad_database', 'bbox')
      self.minLL = None
      self.maxLL = None
      if(bbox != None):
        latLongs = bbox.split(';')
        self.minLL = LatLong()
        self.maxLL = LatLong()
        latlon = latLongs[0].split(',')
        self.minLL.latitude = float( latlon[0] )
        self.minLL.longitude = float( latlon[1] )
        latlon = latLongs[1].split(',')
        self.maxLL.latitude = float( latlon[0] )
        self.maxLL.longitude = float( latlon[1] )

      #Delete data that is older than the LastNDays
      self.xmrgKeepLastNDays = configFile.getint('nexrad_database', 'keepLastNDays')

      #Try to fill in any holes in the data going back N days.
      self.backfillLastNDays = configFile.getint('nexrad_database', 'backfillLastNDays')

      #Flag to specify whether or not to write the precip data to the database.
      self.writePrecipToDB = configFile.getboolean('nexrad_database', 'writeToDB')

      self.writePrecipToKML = configFile.getboolean('nexrad_database', 'writeToKML')

      #If we are going to write shapefiles, get the output directory.
      if(self.writePrecipToKML):
        self.KMLDir = configFile.get('nexrad_database', 'KMLDir')
        if(len(self.KMLDir) == 0):
          self.writePrecipToKML = 0
          if self.logger is not None:
            self.logger.error("No KML directory provided, will not write shapefiles.")

      self.saveAllPrecipVals = configFile.getboolean('nexrad_database', 'saveAllPrecipVals')

      self.createPolygonsFromGrid = configFile.getboolean('nexrad_database', 'createPolygonsFromGrid')

      #Flag to specify if we want to delete the compressed XMRG file when we are done processing.
      #We might not be working off a compressed source file, so this flag only applies to a compressed file.
      self.deleteCompressedSourceFile = configFile.getboolean('nexrad_database', 'deleteCompressedSourceFile')

      #Flag to specify if we want to delete the XMRG file when we are done processing.
      self.deleteSourceFile = configFile.getboolean('nexrad_database', 'deleteSourceFile')

      #Flag to specify if we want to calculate the weighted averages for the watersheds as we write the radar data
      #into the precipitation_radar table.
      self.calcWeightedAvg =configFile.getboolean('nexrad_database', 'calculateWeightedAverage')


      self.dbName = configFile.get('database', 'name')
      self.spatiaLiteLib = configFile.get('database', 'spatiaLiteLib')


      self.baseURL = configFile.get('nexrad_database', 'baseURL')
      #This tag is used to help further refine the files we process. For instance, hourly xmrg files are prepended
      #with xmrg whereas the 6hr and 24hr files aren't. So we could use this to ignore those.
      self.fileNameFilter = configFile.get('nexrad_database', 'fileNameFilter')
      self.xmrgDLDir = configFile.get('nexrad_database', 'downloadDir')

      #Directory where the NEXRAD database schema files live.
      self.nexrad_schema_directory = configFile.get('nexrad_database', 'schema_directory')
      #The files that create the tables we need in our NEXRAD DB.
      self.nexrad_schema_files = configFile.get('nexrad_database', 'schema_files').split(',')

      #File containing the boundaries we want to use to carve out data from the XMRG file.
      self.boundaries_file = configFile.get('boundaries_settings', 'boundaries_file')

      #Number of worker processes to start.
      self.worker_process_count = configFile.getint('nexrad_database', 'worker_process_count')

      #Specifies to attempt to add the sensors before inserting the data. Only need to do this
      #on intial run.
      self.add_sensors = True
      #Specifies to attempt to add the platforms representing the radar coverage.
      self.add_platforms = True

      self.save_boundary_grid_cells = True
      self.save_boundary_grids_one_pass = True

    except (ConfigParser.Error, Exception) as e:
      if self.logger:
        self.logger.exception(e)

    #Process the boundaries
    try:
      header_row = ["WKT", "NAME"]
      if self.logger:
        self.logger.debug("Reading boundaries geometry file: %s" % (self.boundaries_file))

      self.boundaries.load(self.boundaries_file)

      #Create the connection to the xenia database where our final results are stored.
      self.xenia_db = wqDB(self.dbName, type(self).__name__)
      if self.add_platforms:
        org_id = self.xenia_db.organizationExists('nws')
        if org_id == -1:
          org_id =  self.xenia_db.addOrganization({'short_name': 'nws'})
        #Add the platforms to represent the watersheds and drainage basins
        for boundary in self.boundaries:
          #platform_handle = 'nws.%s.radarcoverage' % (boundary['name'])
          platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
          if self.xenia_db.platformExists(platform_handle) == -1:
            if self.xenia_db.addPlatform({'organization_id': org_id,
                                          'platform_handle': platform_handle,
                                          'short_name': boundary.name, #'short_name': boundary['name'],
                                          'active': 1}) == -1:
              self.logger.error("Failed to add platform: %s for org_id: %d, cannot continue" % (platform_handle, org_id))

    except (IOError,Exception) as e:
      if self.logger:
        self.logger.exception(e)


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
    if len(fileParts):
      #Now let's manipulate the string to match the dateformat var above.
      filetime = re.sub("xmrg_", "", fileParts[0])
      filetime = re.sub("_","", filetime)
    else:
      if filetime.find('24hrxmrg') != -1:
        dateformat = "%m%d%Y"
      filetime = filetime.replace('24hrxmrg', '')
      filetime = filetime.replace('xmrg', '')
      filetime = filetime.replace('z', '')
    #Using mktime() and localtime() is a hack. The time package in python doesn't have a way
    #to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
    #I want instead of brining in the calender package which has the conversion.
    secs = time.mktime(time.strptime(filetime, dateformat))
    #secs -= offset
    filetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(secs))
    
    return(filetime)

  def write_boundary_grid_kml(self, boundary, datetime, boundary_grids):
    if self.logger:
      self.logger.info("Start write_boundary_grid_kml for boundary: %s Date: %s" % (boundary, datetime))
    kml_doc = KML.kml(KML.Document(
                        KML.Name("Boundary: %s" % (boundary)),
                        KML.Style(
                          KML.LineStyle(
                            KML.color('ffff0000'),
                            KML.width(3),
                          ),
                          KML.PolyStyle(
                            KML.color('800080ff'),
                          ),
                          id='grid_style'
                        )
                      )
    )

    #doc = etree.SubElement(kml_doc, 'Document')
    try:
      for polygon, val in boundary_grids:
        coords = " ".join("%s,%s,0" % (tup[0],tup[1]) for tup in polygon.exterior.coords[:])
        kml_doc.Document.append(KML.Placemark(KML.name('%f' % val),
                                              KML.styleUrl('grid_style'),
                                               KML.Polygon(
                                                 KML.outerBoundaryIs(
                                                   KML.LinearRing(
                                                    KML.coordinates(coords)
                                                   )
                                                 )
                                               ))
        )
    except (TypeError,Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      try:
        kml_outfile = "%s%s_%s.kml" % (self.KMLDir, boundary, datetime.replace(':', '_'))
        if self.logger:
          self.logger.debug("write_boundary_grid_kml KML outfile: %s" % (kml_outfile))
        kml_file = open(kml_outfile, "w")
        kml_file.write(etree.tostring(kml_doc, pretty_print=True))
        kml_file.close()
      except (IOError,Exception) as e:
        if self.logger:
          self.logger.exception(e)

    if self.logger:
      self.logger.info("End write_boundary_grid_kml for boundary: %s Date: %s" % (boundary, datetime))
    return

  def import_files(self, file_list):
    if self.logger:
      self.logger.debug("Start import_files" )

      workers = self.worker_process_count
      inputQueue = Queue()
      resultQueue = Queue()
      processes = []

      if self.logger:
        self.logger.debug("Importing: %d files." % (len(file_list)))
      for file_name in file_list:
        inputQueue.put(file_name)

      #Start up the worker processes.
      for workerNum in xrange(workers):
        args = {
          'logger': True,
          'input_queue': inputQueue,
          'results_queue': resultQueue,
          'min_lat_lon': self.minLL,
          'max_lat_lon': self.maxLL,
          'nexrad_schema_files': self.nexrad_schema_files,
          'nexrad_schema_directory':self.nexrad_schema_directory,
          'save_all_precip_vals': self.saveAllPrecipVals,
          'boundaries': self.boundaries,
          'spatialite_lib': self.spatiaLiteLib,
          'delete_source_file': self.deleteSourceFile,
          'delete_compressed_source_file': self.deleteCompressedSourceFile
        }
        p = Process(target=process_xmrg_file, kwargs=args)
        if self.logger:
          self.logger.debug("Starting process: %s" % (p._name))
        p.start()
        processes.append(p)
        inputQueue.put('STOP')


      #If we don't empty the resultQueue periodically, the .join() below would block continously.
      #See docs: http://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
      #the blurb on Joining processes that use queues
      rec_count = 0
      while any([checkJob.is_alive() for checkJob in processes]):
        if not resultQueue.empty():

          #finalResults.append(resultQueue.get())
          self.process_result(resultQueue.get())
          rec_count += 1

      #Wait for the process to finish.
      for p in processes:
        p.join()

      #Poll the queue once more to get any remaining records.
      while not resultQueue.empty():
        self.process_result(resultQueue.get())
        rec_count += 1

      if self.logger:
        self.logger.debug("Imported: %d records" % (rec_count))

    if self.logger:
      self.logger.debug("Finished import_files" )

    return

  def importFiles(self, importDirectory=None):
    try:
      if importDirectory is None:
        importDirectory = self.importDirectory

      if self.logger:
        self.logger.debug("Importing from: %s" % (importDirectory))

      workers = self.worker_process_count
      inputQueue = Queue()
      resultQueue = Queue()
      finalResults = []
      processes = []

      fileList = os.listdir(importDirectory)
      fileList.sort()
      #If we want to skip certain months, let's pull those files out of the list.
      monthList = {'Jan': 1, 'Feb': 2, 'Mar': 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12 }
      #startMonth = monthList[startMonth]
      #endMonth = monthList[endMonth]
      for file_name in fileList:
        full_path = "%s%s" % (importDirectory, file_name)
        inputQueue.put(full_path)

      #Start up the worker processes.
      for workerNum in xrange(workers):
        args = {
          'logger': True,
          'input_queue': inputQueue,
          'results_queue': resultQueue,
          'min_lat_lon': self.minLL,
          'max_lat_lon': self.maxLL,
          'nexrad_schema_files': self.nexrad_schema_files,
          'nexrad_schema_directory':self.nexrad_schema_directory,
          'save_all_precip_vals': self.saveAllPrecipVals,
          'boundaries': self.boundaries,
          'spatialite_lib': self.spatiaLiteLib
        }
        p = Process(target=process_xmrg_file, kwargs=args)
        if self.logger:
          self.logger.debug("Starting process: %s" % (p._name))
        p.start()
        processes.append(p)
        inputQueue.put('STOP')


      #If we don't empty the resultQueue periodically, the .join() below would block continously.
      #See docs: http://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
      #the blurb on Joining processes that use queues
      rec_count = 0
      while any([checkJob.is_alive() for checkJob in processes]):
        if(resultQueue.empty() == False):

          #finalResults.append(resultQueue.get())
          self.process_result(resultQueue.get())
          rec_count += 1

      #Wait for the process to finish.
      for p in processes:
        p.join()

      #Poll the queue once more to get any remaining records.
      while(resultQueue.empty() == False):
        self.process_result(resultQueue.get())
        rec_count += 1

      if self.logger:
        self.logger.debug("Finished. Import: %d records from: %s" % (rec_count, importDirectory))

    except Exception, E:
      self.lastErrorMsg = str(E)
      if self.logger is not None:
        self.logger.exception(E)

  def process_result(self, xmrg_results):
    try:
      if self.writePrecipToKML and xmrg_results.get_boundary_grid('complete_area') is not None:
        if self.writePrecipToKML:
          self.write_boundary_grid_kml('complete_area', xmrg_results.datetime, xmrg_results.get_boundary_grid('complete_area'))

      for boundary_name, boundary_results in xmrg_results.get_boundary_data():
        if self.writePrecipToKML and xmrg_results.get_boundary_grid(boundary_name) is not None:
          self.write_boundary_grid_kml(boundary_name, xmrg_results.datetime, xmrg_results.get_boundary_grid(boundary_name))

        platform_handle = "nws.%s.radarcoverage" % (boundary_name)
        lat = 0.0
        lon = 0.0

        avg = boundary_results['weighted_average']
        #self.save_data()
        if avg != None:
          if avg > 0.0 or self.saveAllPrecipVals:
            if avg != -9999:
              mVals = []
              mVals.append(avg)
              if self.add_sensors:
                self.xenia_db.addSensor('precipitation_radar_weighted_average', 'mm',
                                        platform_handle,
                                        1,
                                        0,
                                        1, None, True)
              #Build a dict of m_type and sensor_id for each platform to make the inserts
              #quicker.
              if platform_handle not in self.sensor_ids:
                m_type_id = self.xenia_db.getMTypeFromObsName('precipitation_radar_weighted_average', 'mm', platform_handle, 1)
                sensor_id = self.xenia_db.sensorExists('precipitation_radar_weighted_average', 'mm', platform_handle, 1)
                self.sensor_ids[platform_handle] = {
                  'm_type_id': m_type_id,
                  'sensor_id': sensor_id}


              #Add the avg into the multi obs table. Since we are going to deal with the hourly data for the radar and use
              #weighted averages, instead of keeping lots of radar data in the radar table, we calc the avg and
              #store it as an obs in the multi-obs table.
              add_obs_start_time = time.time()
              if self.xenia_db.addMeasurementWithMType(self.sensor_ids[platform_handle]['m_type_id'],
                                              self.sensor_ids[platform_handle]['sensor_id'],
                                              platform_handle,
                                              xmrg_results.datetime,
                                              lat, lon,
                                              0,
                                              mVals,
                                              1,
                                              True,
                                              self.processingStartTime):
                if self.logger is not None:
                  self.logger.debug("Platform: %s Date: %s added weighted avg: %f in %f seconds." %(platform_handle, xmrg_results.datetime, avg, time.time() - add_obs_start_time))
              else:
                if self.logger is not None:
                  self.logger.error( "%s"\
                                     %(self.xenia_db.getErrorInfo()) )
                self.xenia_db.clearErrorInfo()
            else:
              if self.logger is not None:
                self.logger.debug( "Platform: %s Date: %s weighted avg: %f(mm) is not valid, not adding to database." %(platform_handle, xmrg_results.datetime, avg))
          else:
            if self.logger is not None:
              self.logger.debug( "Platform: %s Date: %s configuration parameter not set to add precip values of 0.0." %(platform_handle, xmrg_results.datetime))
        else:
          if self.logger is not None:
            self.logger.error( "Platform: %s Date: %s Weighted AVG error: %s" %(platform_handle, xmrg_results.datetime, self.xenia_db.getErrorInfo()) )
            self.xenia_db.clearErrorInfo()
      if self.save_boundary_grids_one_pass:
        self.writePrecipToKML = False

    except StopIteration, e:
      if self.logger:
        self.logger.info("Date: %s Boundary data exhausted" % (xmrg_results.datetime))

    return

  def save_data(self):
    return
  def download_file_list(self, file_list):
    files_downloaded = []
    for file_name in file_list:
      remote_filename_url = os.path.join(self.baseURL, file_name)
      if self.logger:
        self.logger.debug("Downloading file: %s" % (remote_filename_url))
      try:
        r = requests.get(remote_filename_url, stream=True)
      except (requests.HTTPError, requests.ConnectionError) as e:
        if self.logger:
          self.logger.exception(e)
      else:
        if r.status_code == 200:
          dest_file = os.path.join(self.xmrgDLDir, file_name)
          if self.logger:
            self.logger.debug("Saving to file: %s" % (dest_file))
          try:
            with open(dest_file, 'wb') as xmrg_file:
              for chunk in r:
                xmrg_file.write(chunk)
          except IOError,e:
            if self.logger:
              self.logger.exception(e)
          files_downloaded.append(dest_file)
        else:
          if self.logger:
            self.logger.error("Unable to download file: %s" % (remote_filename_url))

    return files_downloaded

  def download_range(self, start_date, hour_count):
    files_downloaded = []
    if self.logger:
      self.logger.debug("Starting download_range")
    try:
      file_list = self.file_list_from_date_range(start_date, hour_count)
    except ConfigParser.Error, e:
      if self.logger:
        self.logger.exception(e)
    else:
      files_downloaded = self.download_file_list(file_list)
    if self.logger:
      self.logger.debug("Finished download_range")

    return files_downloaded
  """
  Function: file_list_from_date_range
  Purpose: Given the starting date and the number of hours in the past, this builds a list
   of the xmrg filenames.
  Parameters:
    start_date_time: A datetime object representing the starting time.
    hour_count: An integer for the number of previous hours we want to build file names for.
  Return:
    A list containing the filenames.
  """
  def file_list_from_date_range(self, start_date_time, hour_count):
    file_list = []
    for x in range(hour_count):
      hr = x + 1
      date_time = start_date_time - timedelta(hours=hr)
      file_name = self.build_filename(date_time)

      file_list.append(file_name)

    return file_list

  def build_filename(self, date_time):
      return date_time.strftime('xmrg%m%d%Y%Hz.gz')

  def fill_gaps(self, start_date_time, hour_count):
    if self.logger:
      self.logger.debug("Starting fill_gaps for start time: %s Previous Hours: %d" % (start_date_time, hour_count))
    time_list = []
    #Build list of times that make up the range.
    for x in range(hour_count):
      hr = x + 1
      date_time = start_date_time - timedelta(hours=hr)
      #time_list.append(date_time.strftime('%Y-%m-%dT%H:%M:%S'))
      time_list.append(date_time)

    begin_date = start_date_time - timedelta(hours=hour_count)
    end_date = start_date_time
    boundary_missing_times = {}
    for boundary in self.boundaries:
      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      boundary_missing_times[boundary.name] = time_list[:]
      try:
        sql = "SELECT m_date FROM multi_obs WHERE m_date >= '%s' AND m_date < '%s' AND platform_handle='%s' ORDER BY m_date"\
        % (begin_date.strftime('%Y-%m-%dT%H:%M:%S'), end_date.strftime('%Y-%m-%dT%H:%M:%S'), platform_handle)
        dbCursor = self.xenia_db.DB.cursor()
        dbCursor.execute( sql )
        for row in dbCursor:
          db_datetime = timezone('UTC').localize(datetime.strptime(row['m_date'], '%Y-%m-%dT%H:%M:%S'))
          if db_datetime in time_list:
            boundary_missing_times[boundary.name].remove(db_datetime)

        if self.logger:
          self.logger.debug("Boundary: %s needs %d files to fill gap." % (boundary.name, len(boundary_missing_times[boundary.name])))
        dbCursor.close()
      except Exception,e:
        if self.logger:
          self.logger.exception(e)
    #Now build a non duplicate time list.
    date_time_unions = []
    for boundary_name in boundary_missing_times:
      date_time_unions = list(set(date_time_unions) | set(boundary_missing_times[boundary_name]))

    date_time_unions.sort()
    if self.logger:
      self.logger.debug("Times missing: %s" % (",".join([dts.strftime("%Y-%m-%dT%H:%M:%S") for dts in date_time_unions])))

    dl_file_list = [self.build_filename(date_time) for date_time in date_time_unions]

    if self.logger:
      self.logger.debug("Files to D/L: %s" % (str(dl_file_list)))

    files_downloaded = self.download_file_list(dl_file_list)

    self.import_files(files_downloaded)
    if self.logger:
      self.logger.debug("Finished fill_gaps for start time: %s Previous Hours: %d" % (start_date_time, hour_count))

    return
  """
  Function: vacuumDB
  Purpose: Frees up unused space in the database.
  """
  def vacuumDB(self):

    retVal = False
    if self.logger is not None:
      stats = os.stat(self.dbName)
      self.logger.debug("Begin database vacuum. File size: %d" % (stats[ST_SIZE]))
    db = wqDB(self.dbSettings.dbName, None, self.logger)
    if(db.vacuumDB() != None):
      if self.logger is not None:
        stats = os.stat(self.dbSettings.dbName)
        self.logger.debug("Database vacuum completed. File size: %d" % (stats[ST_SIZE]))
      retVal = True
    else:
      self.logger.error("Database vacuum failed: %s" % (db.lastErrorMsg))
      db.lastErrorMsg = ""
    db.DB.close()
    return(retVal)


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportData", dest="import_data",
                    help="Directory to import XMRG files from" )
  parser.add_option("-b", "--BackfillNHours", dest="backfill_n_hours", type="int",
                    help="Number of hours of NEXRAD data to download and process." )
  parser.add_option("-g", "--FillGaps", dest="fill_gaps",  action="store_true", default=False,
                    help="If set, this will find gaps for the past N hours as defined in the BackfillNHours." )
  parser.add_option("-s", "--StartDate", dest="start_date", default=None,
                    help="Options starting date to use for backfill or gap find operations." )

  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.config_file)

    logger = None
    logConfFile = configFile.get('logging', 'xmrg_ingest')
    logger_name = configFile.get('logging', 'xmrg_ingest_logger_name')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger(logger_name)
      logger.info("Log file opened.")
  except ConfigParser.Error, e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  except Exception,e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    xmrg_proc = wqXMRGProcessing(logger=True)
    xmrg_proc.load_config_settings(config_file = options.config_file)
    if options.import_data is not None:
      if logger:
        logger.info("Importing directory: %s" % (options.import_data))

      import_dirs = options.import_data.split(",")

      for import_dir in import_dirs:
        file_list = os.listdir(import_dir)
        file_list.sort()
        #If we have provided a starting date and time, remove the files that are older than
        #the provided date.

        if options.start_date is not None:
          starting_file_list = []
          starting_datetime = datetime.strptime(options.start_date, '%Y-%m-%dT%H:%M:%S')
          for file_ndx in range(len(file_list)):
            if file_list[file_ndx].find('xmrg') != -1:
              file_date = datetime.strptime(getCollectionDateFromFilename(file_list[file_ndx]), '%Y-%m-%dT%H:%M:%S')
              if file_date >= starting_datetime:
                starting_file_list = file_list[file_ndx:-1]
                break
          file_list = starting_file_list[:]
        full_path_file_list = [os.path.join(import_dir, file_name) for file_name in file_list]
        xmrg_proc.import_files(full_path_file_list)

    elif options.fill_gaps:
      if options.start_date is None:
        start_date_time = timezone('US/Eastern').localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).astimezone(timezone('UTC'))
      else:
        start_date_time = timezone('UTC').localize(datetime.strptime(options.start_date, "%Y-%m-%dT%H:%M:%S"))
      if logger:
        logger.info("Fill gaps Start time: %s Prev Hours: %d" % (start_date_time, options.backfill_n_hours))

      xmrg_proc.fill_gaps(start_date_time, options.backfill_n_hours)

    elif options.backfill_n_hours:
      start_date_time = timezone('US/Eastern').localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).astimezone(timezone('UTC'))
      if logger:
        logger.info("Backfill N Hours Start time: %s Prev Hours: %d" % (start_date_time, options.backfill_n_hours))
      file_list = xmrg_proc.download_range(start_date_time, options.backfill_n_hours)
      xmrg_proc.import_files(file_list)

  if logger:
    logger.info("Log file closed.")

if __name__ == "__main__":
  main()
