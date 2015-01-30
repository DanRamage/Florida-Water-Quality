import os
import optparse
from shapely.geometry import Polygon
from shapely.wkt import loads as wkt_loads
import csv


def read_boundaries(boundaries_file):
  boundaries = []
  header_row = ["WKT", "NAME"]
  print("Reading boundaries geometry file: %s" % (boundaries_file))

  with open(boundaries_file, "rU") as geometry_file:
    dict_file = csv.DictReader(geometry_file, delimiter=',', quotechar='"', fieldnames=header_row)
    line_num = 0
    for row in dict_file:
      if line_num > 0:
        print("Building boundary polygon for: %s" % (row['NAME']))
        boundary_poly = wkt_loads(row['WKT'])
        boundaries.append({'name': row['NAME'], 'polygon': boundary_poly})
      line_num += 1

  return boundaries

def read_points(station_file):
  station_points = []
  header_row = ["WKT", "EPAbeachID", "SPLocation"]

  with open(station_file, "rU") as geometry_file:
    dict_file = csv.DictReader(geometry_file, delimiter=',', quotechar='"', fieldnames=header_row)
    line_num = 0
    for row in dict_file:
      if line_num > 0:
        print("Building station point for: %s" % (row['SPLocation']))
        station_point = wkt_loads(row['WKT'])
        station_points.append({'name': row['SPLocation'], 'point': station_point})
      line_num += 1

  return station_points

  return points
def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--BoundariesFile", dest="boundaries_file",
                    help="CSV File with the boundaries we are testing against." )
  parser.add_option("-i", "--PointsFile", dest="points_file",
                    help="File with the sample sites we are trying to match to the boundaries." )
  parser.add_option("-0", "--OutFile", dest="out_file",
                    help="File for boundaries with stations." )
  (options, args) = parser.parse_args()

  if(options.boundaries_file is None and options.points_file):
    parser.print_help()
    sys.exit(-1)

  boundaries = read_boundaries(options.boundaries_file)
  station_points  = read_points(options.points_file)

  for station_point in station_points:
    #print("Station: %s" % (station_point['name']))
    station_in_polygon = False
    for boundary in boundaries:
      #print("Boundary: %s" % (boundary['name']))
      if boundary['polygon'].contains(station_point['point']):
        print("Station: %s in Boundary: %s" % (station_point['name'], boundary['name']))
        if 'station_list' not in boundary:
          boundary['station_list'] = []
        boundary['station_list'].append(station_point)
        station_in_polygon = True
    if not station_in_polygon:
      print("ERROR: Station: %s did not fall inside a boundary." % (station_point['name']))


  try:
    with open(options.out_file, "w") as out_file:
      out_file.write("WKT,NAME,SAMPLE_SITES\n")
      for boundary in boundaries:
        stations = []
        if 'station_list' in boundary:
          stations = [station['name'] for station in boundary['station_list']]
        out_file.write("\"%s\",\"%s\",\"%s\"\n" % (boundary['polygon'].wkt, boundary['name'], ",".join(stations)))
  except IOError, e:
    import traceback
    traceback.print_exc(e)
  return


if __name__ == "__main__":
  main()