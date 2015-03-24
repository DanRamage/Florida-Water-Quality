from shapely.geometry import Point, Polygon
import numpy as np
from matplotlib import path

def bbox2ij(lon,lat,bbox):
    """Return indices for i,j that will completely cover the specified bounding box.     
    i0,i1,j0,j1 = bbox2ij(lon,lat,bbox)
    lon,lat = 2D arrays that are the target of the subset
    bbox = list containing the bounding box: [lon_min, lon_max, lat_min, lat_max]

    Example
    -------  
    >>> i0,i1,j0,j1 = bbox2ij(lon_rho,[-71, -63., 39., 46])
    >>> h_subset = nc.variables['h'][j0:j1,i0:i1]       
    """
    bbox=np.array(bbox)
    mypath=np.array([bbox[[0,1,1,0]],bbox[[2,2,3,3]]]).T
    p = path.Path(mypath)
    points = np.vstack((lon.flatten(),lat.flatten())).T   
    n,m = np.shape(lon)
    inside = p.contains_points(points).reshape((n,m))
    ii,jj = np.meshgrid(xrange(m),xrange(n))
    return min(ii[inside]),max(ii[inside]),min(jj[inside]),max(jj[inside])

def closestCellFromPt(lon, lat, lonArray, latArray, obsArray, obsFillValue, landMaskArray):
  latlon = Point(lon,lat)
  lastDistance = None
  n,m = np.shape(lonArray)
  cellPoint = None
  for i in range(0,n):
    for j in range(0,m):
      #We have to be in the water, and the cell has to have a valid observation values.
      if(landMaskArray[i,j] == 1 and obsArray[i,j] != obsFillValue):
        gridPt = Point(lonArray[i,j],latArray[i,j])
        curDist = latlon.distance(gridPt)      
        if(lastDistance == None or curDist < lastDistance):
          lastDistance = curDist
          cellPoint = Point(i,j)
      
  return(cellPoint)  

def closestCellFromPtInPolygon(lon_lat, lon_array, lat_array, obs_array, fill_value, containing_polygon):
  cell_point = None
  last_dist = None
  for x in range(0, len(lon_array)):
    for y in range(0, len(lat_array)):
      grid_pt = Point(lon_array[x], lat_array[y])
      if obs_array[y,x] != fill_value and grid_pt.within(containing_polygon):
        cur_dist = lon_lat.distance(grid_pt)
        if last_dist == None or cur_dist < last_dist:
          last_dist = cur_dist
          cell_point = Point(x,y)
  return cell_point