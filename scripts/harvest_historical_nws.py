
import requests
from datetime import datetime, timedelta
import traceback

def main():
  station_id = 12871
  #http://cdo.ncdc.noaa.gov/qclcd/QCLCD?VARVALUE=12871200501&prior=N&qcname=VER2&reqday=E&reqday=&stnid=n%2Fa&which=ASCII%20Download%20(Hourly%20Obs.)%20(10A)
  base_url = 'http://cdo.ncdc.noaa.gov/qclcd/QCLCD?'
  dest_dir = '/Users/danramage/Documents/workspace/WaterQuality/Florida_Water_Quality/data/in-situ/nws-ksrq/months/'
  start_date = datetime.strptime('2015-01-01', '%Y-%m-%d')
  end_date = datetime.strptime('2015-07-01', '%Y-%m-%d')
  next_month = start_date
  start_month = 1
  start_year = 2005
  while next_month < end_date:
    station_data = '%d%s' % (station_id, next_month.strftime('%Y%m'))
    params = {
      'VARVALUE':	station_data,
      'prior':	'N',
      'qcname':	'VER2',
      'reqday':	'E',
      'stnid':	'n/a',
      'which':	'ASCII Download (Hourly Obs.) (10A)'
    }
    try:
      dest_file = '%s%s.csv' % (dest_dir, next_month.strftime('%Y_%m'))
      with open(dest_file, "w") as out_file:
        data_req = requests.post(base_url, data=params)
        out_file.write(data_req.text)
    except (IOError, requests.ConnectionError) as e:
      traceback.print_exc(e)

    start_month += 1
    if start_month > 12:
      start_month = 1
      start_year += 1
    next_month = datetime.strptime(('%d-%02d-01' % (start_year, start_month)), '%Y-%m-%d')

  return

if __name__ == "__main__":
  main()