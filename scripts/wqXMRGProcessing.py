import sys
sys.path.append('../commonfiles')

import os
import logging.config
import time
import optparse
import ConfigParser
import time
import re
import csv
from pysqlite2 import dbapi2 as sqlite3
from multiprocessing import Lock, Process, Queue, current_process
from shapely.geometry import Polygon
from shapely.wkt import loads as wkt_loads
from pykml.factory import KML_ElementMaker as KML
from lxml import etree

from dhecDB import dhecDB, nexrad_db
from processXMRGFile import processXMRGData
from xmrgFile import xmrgFile,hrapCoord,LatLong

class configSettings(object):
  def __init__(self, config_file):
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(config_file)

      bbox = configFile.get('processing_settings', 'bbox')
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
      self.xmrgKeepLastNDays = configFile.getint('processing_settings', 'keepLastNDays')

      #Try to fill in any holes in the data going back N days.
      self.backfillLastNDays = configFile.getint('processing_settings', 'backfillLastNDays')

      #Flag to specify whether or not to write the precip data to the database.
      self.writePrecipToDB = configFile.getboolean('processing_settings', 'writeToDB')

      self.writePrecipToKML = configFile.getboolean('processing_settings', 'writeToKML')

      #If we are going to write shapefiles, get the output directory.
      if(self.writePrecipToKML):
        self.KMLDir = configFile.get('processing_settings', 'KMLDir')
        if(len(self.KMLDir) == 0):
          self.writePrecipToKML = 0
          if self.logger is not None:
            self.logger.error("No KML directory provided, will not write shapefiles.")

      self.saveAllPrecipVals = configFile.getboolean('processing_settings', 'saveAllPrecipVals')

      self.createPolygonsFromGrid = configFile.getboolean('processing_settings', 'createPolygonsFromGrid')

      #Flag to specify if we want to delete the compressed XMRG file when we are done processing.
      #We might not be working off a compressed source file, so this flag only applies to a compressed file.
      self.deleteCompressedSourceFile = configFile.getboolean('processing_settings', 'deleteCompressedSourceFile')

      #Flag to specify if we want to delete the XMRG file when we are done processing.
      self.deleteSourceFile = configFile.getboolean('processing_settings', 'deleteSourceFile')

      #Directory to import XMRG files from
      self.importDirectory = configFile.get('processing_settings', 'importDirectory')

      #Flag to specify if we want to calculate the weighted averages for the watersheds as we write the radar data
      #into the precipitation_radar table.
      self.calcWeightedAvg =configFile.getboolean('processing_settings', 'calculateWeightedAverage')


      self.dbName = configFile.get('database', 'name')
      self.spatiaLiteLib = configFile.get('database', 'spatiaLiteLib')


      self.baseURL = configFile.get('processing_settings', 'baseURL')
      #This tag is used to help further refine the files we process. For instance, hourly xmrg files are prepended
      #with xmrg whereas the 6hr and 24hr files aren't. So we could use this to ignore those.
      self.fileNameFilter = configFile.get('processing_settings', 'fileNameFilter')
      self.xmrgDLDir = configFile.get('processing_settings', 'downloadDir')

      #Directory where the NEXRAD database schema files live.
      self.nexrad_schema_directory = configFile.get('nexrad_database', 'schema_directory')
      #The files that create the tables we need in our NEXRAD DB.
      self.nexrad_schema_files = configFile.get('nexrad_database', 'schema_files').split(',')

      #File containing the boundaries we want to use to carve out data from the XMRG file.
      self.boundaries_file = configFile.get('boundaries_settings', 'boundaries_file')

      #Specifies to attempt to add the sensors before inserting the data. Only need to do this
      #on intial run.
      self.add_sensors = True
      #Specifies to attempt to add the platforms representing the radar coverage.
      self.add_platforms = True

      self.save_boundary_grid_cells = True
      self.save_boundary_grids_one_pass = True

    except (ConfigParser.Error, Exception) as e:
      raise e

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
                """
                use_record = False
                if minLatLong is not None and maxLatLong is not None:
                  if xmrg.inBBOX(latlon, minLatLong, maxLatLong):
                    use_record = True
                else:
                  use_record = True
                """
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
                if save_boundary_grid_cells:
                  boundary_grid_query_start = time.time()
                  cells_cursor = nexrad_db_conn.get_radar_data_for_boundary(boundary['polygon'], filetime, filetime)
                  for row in cells_cursor:
                    cell_poly = wkt_loads(row['WKT'])
                    precip = row['precipitation']
                    results.add_grid(boundary['name'], (cell_poly, precip))

                  if logger:
                    logger.debug("ID: %s(%f secs) to query grids for boundary: %s"\
                                 % (current_process().name, time.time() - boundary_grid_query_start, boundary['name']))


                avg_start_time = time.time()
                avg = nexrad_db_conn.calculate_weighted_average(boundary['polygon'], filetime, filetime)
                results.add_boundary_result(boundary['name'], 'weighted_average', avg)
                avg_total_time = time.time() - avg_start_time
                if logger:
                  logger.debug("ID: %s(%f secs) to process average for boundary: %s"\
                               % (current_process().name, avg_total_time, boundary['name']))

            resultsQueue.put(results)

            nexrad_db_conn.delete_all()
          #Only do it for one file. Following files should all be same results other than the precip values.
          if save_boundary_grids_one_pass:
            save_boundary_grid_cells = False
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
class wqXMRGProcessing(processXMRGData):
  def __init__(self, logger=True):

    self.logger = None
    if logger:
      self.logger = logging.getLogger(type(self).__name__)
      self.xenia_db = None
      self.boundaries = []
      self.sensor_ids = {}
    try:
      #2011-07-25 DWR
      #Added a processing start time to use for the row_entry_date value when we add new records to the database.
      self.processingStartTime = time.strftime('%Y-%d-%m %H:%M:%S', time.localtime())
      self.configSettings = None

    except (ConfigParser.Error, Exception) as e:
      if self.logger is not None:
        self.logger.exception(e)
      raise e

  def load_config_settings(self, **kwargs):
    self.configSettings = configSettings(kwargs['config_file'])
    #Process the boundaries
    try:
      header_row = ["WKT", "NAME"]
      if self.logger:
        self.logger.debug("Reading boundaries geometry file: %s" % (self.configSettings.boundaries_file))

      geometry_file = open(self.configSettings.boundaries_file, "rU")
      dict_file = csv.DictReader(geometry_file, delimiter=',', quotechar='"', fieldnames=header_row)
      line_num = 0
      for row in dict_file:
        if line_num > 0:
          if self.logger:
            self.logger.debug("Building boundary polygon for: %s" % (row['NAME']))
          boundary_poly = wkt_loads(row['WKT'])
          self.boundaries.append({'name': row['NAME'], 'polygon': boundary_poly})
        line_num += 1

      #Create the connection to the xenia database where our final results are stored.
      self.xenia_db = dhecDB(self.configSettings.dbName, type(self).__name__)
      if self.configSettings.add_platforms:
        org_id = self.xenia_db.organizationExists('nws')
        if org_id == -1:
          org_id =  self.xenia_db.addOrganization({'short_name': 'nws'})
        #Add the platforms to represent the watersheds and drainage basins
        for boundary in self.boundaries:
          platform_handle = 'nws.%s.radarcoverage' % (boundary['name'])
          if self.xenia_db.platformExists(platform_handle) == -1:
            if self.xenia_db.addPlatform({'organization_id': org_id,
                                          'platform_handle': platform_handle,
                                          'short_name': boundary['name'],
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
        kml_outfile = "%s%s_%s.kml" % (self.configSettings.KMLDir, boundary, datetime.replace(':', '_'))
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

  def importFiles(self, importDirectory=None):
    try:
      if importDirectory is None:
        importDirectory = self.importDirectory

      if self.logger:
        self.logger.debug("Importing from: %s" % (importDirectory))

      workers = 4
      inputQueue = Queue()
      resultQueue = Queue()
      finalResults = []
      processes = []

      fileList = os.listdir(importDirectory)
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
          'min_lat_lon': self.configSettings.minLL,
          'max_lat_lon': self.configSettings.maxLL,
          'nexrad_schema_files': self.configSettings.nexrad_schema_files,
          'nexrad_schema_directory':self.configSettings.nexrad_schema_directory,
          'save_all_precip_vals': self.configSettings.saveAllPrecipVals,
          'boundaries': self.boundaries,
          'spatialite_lib': self.configSettings.spatiaLiteLib
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
      addSensor = True

      if self.configSettings.writePrecipToKML and xmrg_results.get_boundary_grid('complete_area') is not None:
        if self.configSettings.writePrecipToKML:
          self.write_boundary_grid_kml('complete_area', xmrg_results.datetime, xmrg_results.get_boundary_grid('complete_area'))

      for boundary_name, boundary_results in xmrg_results.get_boundary_data():
        if self.configSettings.writePrecipToKML and xmrg_results.get_boundary_grid(boundary_name) is not None:
          self.write_boundary_grid_kml(boundary_name, xmrg_results.datetime, xmrg_results.get_boundary_grid(boundary_name))

        platform_handle = "nws.%s.radarcoverage" % (boundary_name)
        lat = 0.0
        lon = 0.0

        avg = boundary_results['weighted_average']
        if avg != None:
          if avg > 0.0 or self.configSettings.saveAllPrecipVals:
            if avg != -9999:
              mVals = []
              mVals.append(avg)
              if self.configSettings.add_sensors:
                self.xenia_db.addSensor('precipitation_radar_weighted_average', 'mm',
                                        platform_handle,
                                        1,
                                        0,
                                        1, None, True)
              if platform_handle not in self.sensor_ids:
                m_type_id = self.xenia_db.getMTypeFromObsName('precipitation_radar_weighted_average', 'mm', platform_handle, 1)
                sensor_id = self.xenia_db.sensorExists('precipitation_radar_weighted_average', 'mm', platform_handle, 1)
                self.sensor_ids[platform_handle] = {
                  'm_type_id': m_type_id,
                  'sensor_id': sensor_id}
              #Add the avg into the multi obs table. Since we are going to deal with the hourly data for the radar and use
              #weighted averages, instead of keeping lots of radar data in the radar table, we calc the avg and
              #store it as an obs in the multi-obs table.
              """
              if self.xenia_db.addMeasurement('precipitation_radar_weighted_average', 'mm',
                                                 platform_handle,
                                                 xmrg_results.datetime,
                                                 lat, lon,
                                                 0,
                                                 mVals,
                                                 1,
                                                 False,
                                                 self.processingStartTime) != True:
              """
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
                return(False)
              recsAdded = True
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
      if self.configSettings.save_boundary_grids_one_pass:
        self.configSettings.writePrecipToKML = False

    except StopIteration, e:
      if self.logger:
        self.logger.info("Date: %s Boundary data exhausted" % (xmrg_results.datetime))

    return
  """
  Function: vacuumDB
  Purpose: Frees up unused space in the database.
  """
  def vacuumDB(self):

    retVal = False
    if self.logger is not None:
      stats = os.stat(self.configSettings.dbName)
      self.logger.debug("Begin database vacuum. File size: %d" % (stats[ST_SIZE]))
    db = dhecDB(self.configSettings.dbSettings.dbName, None, self.logger)
    if(db.vacuumDB() != None):
      if self.logger is not None:
        stats = os.stat(self.configSettings.dbSettings.dbName)
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
    if len(options.import_data):
      xmrg_proc = wqXMRGProcessing(logger=True)
      xmrg_proc.load_config_settings(config_file = options.config_file)
      xmrg_proc.importFiles(options.import_data)


if __name__ == "__main__":
  main()
