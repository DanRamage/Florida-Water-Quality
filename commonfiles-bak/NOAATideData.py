import sys
sys.path.append('../commonfiles')

from suds.client import Client
from suds import WebFault
import time
from datetime import datetime
from pytz import timezone as pytz_timezone
from date_time_utils import get_utc_epoch, datetime2matlabdn
import array

#import matplotlib
#matplotlib.use('Agg')
#import matplotlib.pyplot as plt

#import numpy as np

class noaaTideData(object):
  LOW_TIDE = 0
  HI_TIDE = 1
  
  def __init__(self, use_raw=True, logger=None):
    self.tideChangesRawData = []
    self.tideChangesSmoothData = []
    self.use_raw = use_raw
    self.baseUrl = 'http://opendap.co-ops.nos.noaa.gov/axis/webservices/waterlevelrawsixmin/wsdl/WaterLevelRawSixMin.wsdl'
    if not self.use_raw:
      self.baseUrl = "http://opendap.co-ops.nos.noaa.gov/axis/webservices/waterlevelverifiedsixmin/wsdl/WaterLevelVerifiedSixMin.wsdl"
    self.logger = logger     

  """
  beginDate is the date we wish to start our data query at. Format is: YYYYMMDD
  endDate is the date we wish to end our data query at. Format is: YYYYMMDD
  waterLevelSensorDataType is the type of water level we want. Can be: 
    W1 = Six minute interval data
    W2 = Hourly water level data
    W3 = Daily high/low water level data
    W4 = Monthly high/low water level data
  relative
  datum A tidal datum is a standard elevation used as a reference to measure water levels. 
    MLLW, or Mean Lower Low Water, is the default datum. Other datum options  are MHHW, MHW, MTW, MSL, MLW, and Station Datum.
  unit is the units of measurement, options are feet or meters.
  shift is the time zone the data should be in. Can be: Local, GMT, LST  
  station is the station name in the form of "ID Name, State" for example: 8661070 Springmaid Pier,SC&
  type is Historic Tide Data, most likely this changes when looking for predicted ranges.
  format can be View Data or View Plot
  """
  def getWaterLevelRawSixMinuteData(self,
                                    beginDate, 
                                    endDate, 
                                    station,
                                    datum='MLLW', 
                                    unit='feet',                                    
                                    shift='GMT' ):
    soapClient = Client(self.baseUrl)
    if(unit == 'feet'):
      unit = 1
    else:
      unit = 2
    if(shift == 'GMT'):
      shift = 0
    else:
      shift = 1

    data = soapClient.service.getWaterLevelRawSixMin(station, beginDate, endDate, datum, unit, shift)

    return(data)

  """
  beginDate is a datetime object we wish to start our data query at.
  endDate is a datetime object we wish to end our data query at.
  waterLevelSensorDataType is the type of water level we want. Can be:
    W1 = Six minute interval data
    W2 = Hourly water level data
    W3 = Daily high/low water level data
    W4 = Monthly high/low water level data
  relative
  datum A tidal datum is a standard elevation used as a reference to measure water levels.
    MLLW, or Mean Lower Low Water, is the default datum. Other datum options  are MHHW, MHW, MTW, MSL, MLW, and Station Datum.
  unit is the units of measurement, options are feet or meters.
  shift is the time zone the data should be in. Can be: Local, GMT, LST
  station is the station name in the form of "ID Name, State" for example: 8661070 Springmaid Pier,SC&
  type is Historic Tide Data, most likely this changes when looking for predicted ranges.
  format can be View Data or View Plot
  """
  def getWaterLevelVerifiedSixMinuteData(self,
                                    beginDate,
                                    endDate,
                                    station,
                                    datum='MLLW',
                                    unit='feet',
                                    shift='GMT' ):
    soapClient = Client(self.baseUrl, timeout=90)
    if(unit == 'feet'):
      unit = 1
    else:
      unit = 2
    if(shift == 'GMT'):
      shift = 0
    else:
      shift = 1

    data = soapClient.service.getWaterLevelVerifiedSixMin(station, beginDate, endDate, datum, unit, shift)

    return(data)

  def calcTideRange(self,           
                    beginDate, 
                    endDate, 
                    station,
                    datum='MLLW', 
                    units='feet',
                    timezone='GMT',
                    smoothData=False,                    
                    tideFileDir=None):

    #This is the dictionary we return. Its keys are the tide indicators: LL is Lowest Low Tide, L is Low Tide, HH Highest High Tide, H High tide.
    tideData = {}
    tideData['LL'] = None
    tideData['HH'] = None
    tideData['L'] = None
    tideData['H'] = None
    tideData['PeakValue'] = None
    tideData['ValleyValue'] = None

    try:
      if self.use_raw:
        wlData = self.getWaterLevelRawSixMinuteData(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
      else:
        wlData = self.getWaterLevelVerifiedSixMinuteData(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
    except (WebFault,Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:

      smoothDataROC = array.array('d')
      rawDataROC = array.array('d')
      expSmoothedData =  array.array('d')
      #tidePts = array.array('d')
      #timePts = array.array('d')
      dataLen = len(wlData.item)
      ndx = 0
      alpha = 0.5
      utc_tz = pytz_timezone('UTC')
      start_ndx = None
      end_ndx = None
      for ndx in range(0, dataLen):
        wl_time = utc_tz.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= beginDate:
          start_ndx = ndx
        if end_ndx is None and wl_time > endDate:
          end_ndx = ndx-1
      wlData.item = wlData.item[start_ndx:end_ndx]
      dataLen = len(wlData.item)
      for ndx in range(0, dataLen):
        valN = wlData.item[ndx]['WL']
        #tidePts.append(valN)
        #data_ts = utc_tz.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        #timePts.append(int(get_utc_epoch(data_ts)))
        #Then the formula for each successive point is (alpha * Xn) + (1-alpha) * Yn-1
        #X is the original data, Yn-1 is the last smoothed data point, alpha is the smoothing constant.
        if ndx == 0:
          expSmoothedData.append(valN)
          tideMin1 = valN
          tideMax1 = valN
          tideMin2 = valN
          tideMax2 = valN

        else:
          timeStruct = utc_tz.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
          timeN = int(get_utc_epoch(timeStruct))
          timeStruct = utc_tz.localize(datetime.strptime(wlData.item[ndx-1]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
          timeN1 = int(get_utc_epoch(timeStruct))
          #timeStruct = utc_tz.localize(time.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
          #timeN = time.mktime(timeStruct)
          #timeStruct = utc_tz.localize(time.strptime(wlData.item[ndx-1]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
          #timeN1 = time.mktime(timeStruct)

          #For each N+1 we now use the formula.
          Yn = (alpha * wlData.item[ndx]['WL']) + ((1 - alpha) * expSmoothedData[ndx-1])
          expSmoothedData.append(Yn)

          smoothDataROC.append((expSmoothedData[ndx] - expSmoothedData[ndx-1]) / (timeN - timeN1))

          #Calcuate the rateofchange
          #ROC for the raw data.
          valN1 = wlData.item[ndx-1]['WL']
          rawDataROC.append((valN - valN1) / (timeN - timeN1))



      ndx = 0
      a = None
      b = None
      c = None
      dirChangeCnt = 0
      chordLen = 10
      midPt = chordLen / 2
      ptFound = False
      stopProc = False
      dataLen = len(wlData.item)
      slopePositive = False

      #plt.plot(timePts,tidePts,'o', x_new, y_new)
      ##plt.xlim([timePts[0]-1, timePts[-1] + 1 ])
      #plt.savefig('/users/danramage/tmp/out.png', dpi=96)

      if(self.logger != None):
        self.logger.info("Checking Raw data.")

      tideChange = None
      changeNdx = None
      lastSlope = None
      for ndx in range(0, len(wlData.item)):
        a = wlData.item[ndx]['WL']
        timeStamp = wlData.item[ndx].timeStamp
        if ndx + chordLen < dataLen - 1:
          c = wlData.item[ndx+chordLen]['WL']
        else:
          stopProc = True
        if tideChange is None:
          tideChange = a
          tide_change_ts = timeStamp
        if not stopProc:
          #Calc slope
          #Ascending
          if c - a > 0:
            if lastSlope == 0:
              if tideData['LL'] is None:
                #tideData['LL'] = tideChange
                tideData['LL'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }
              elif tideChange < tideData['LL']['value']:
                tmp = tideData['LL']
                #tideData['LL'] = tideChange
                tideData['LL'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }
                tideData['L'] = tmp
              else:
                tideData['L'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }

              #print("Tide Min at: %f@%s" %(tideChange,timeStamp))
              if self.logger:
                self.logger.debug("Tide Min at: %f@%s" %(tideChange,tide_change_ts))
              #Found the max tide, so another is not going to occur any quicker than the chord length, so increment the ndx.
              ndx += chordLen
              #Slope has changed direction.
              lastSlope = 1
              continue
            lastSlope = 1

            if(a > tideChange):
              tideChange = a
              tide_change_ts = timeStamp
              changeNdx = ndx
          #Descending
          elif c - a < 0:
            if lastSlope == 1:
              if tideData['HH'] is None:
                #tideData['HH'] = tideChange
                tideData['HH'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }

              elif tideChange > tideData['HH']['value']:
                tmp = tideData['HH']
                #tideData['HH'] = tideChange
                tideData['HH'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }
                tideData['H'] = tmp
              else:
                tideData['H'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }

              #print("Tide Max at: %f@%s" %(tideChange,timeStamp))
              if self.logger:
                self.logger.debug("Tide Max at: %f@%s" %(tideChange,tide_change_ts))

              #Found the max tide, so another is not going to occur any quicker than the chord length, so increment the ndx.
              ndx += chordLen
              #Slope has changed direction.
              lastSlope = 0
              continue
            lastSlope = 0

            if a < tideChange:
              tideChange = a
              tide_change_ts = timeStamp

              changeNdx = ndx
        #Save off the highest and lowest values.
        if tideData['PeakValue'] is None or tideData['PeakValue']['value'] < a:
          tideData['PeakValue'] = {'value': a,
                                    'date': timeStamp}
        if tideData['ValleyValue'] is None or tideData['ValleyValue']['value'] > a:
          tideData['ValleyValue'] = {'value': a,
                                    'date': timeStamp}

        ndx += 1

      if smoothData:
        print("Checking smoothed data.")
        dataLen = len(expSmoothedData)
        ndx = 0
        ptFound = False
        stopProc = False

        while ndx < dataLen:
          a = expSmoothedData[ndx]
          if ndx + midPt < dataLen - 1:
            b = expSmoothedData[ndx+midPt]
          else:
            stopProc = True
          if ndx + chordLen < dataLen - 1:
            c = expSmoothedData[ndx+chordLen]
          else:
            stopProc = True
          if stopProc == False:
            #Calc slope
            if c - a > 0:
              if b > a and b > c:
                #print("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                if self.logger != None:
                  self.logger.debug("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))

                ptFound = True
            elif c - a < 0:
              if b < a and b < c:
                #print("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                if self.logger:
                  self.logger.debug("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                ptFound = True
          if ptFound == False:
            ndx += 1
          else:
            ndx = ndx+midPt
            ptFound = False

      if tideFileDir != None:
        filename = "%s\\%s-%s.csv" %(tideFileDir,beginDate,endDate)
        tideFile = open(filename, "w")

        ndx = 0
        dataLen = len(wlData.item)
        while ndx < dataLen:
          timeStruct = time.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0')
          seconds = time.mktime(timeStruct)
          medianROC = ''
          rawROC = ''
          smoothedData = ''
          if(ndx < len(rawDataROC)):
            rawROC = rawDataROC[ndx]
            smoothedROC = smoothDataROC[ndx]
            smoothedData = expSmoothedData[ndx]

          outbuf = "%s,%s,%s,%s,%s\n" %(seconds,wlData.item[ndx]['WL'], rawROC, smoothedData, smoothedROC)
          ndx += 1
          tideFile.write(outbuf)
        tideFile.close()
    #If we didn't have all the inflection points, we'll use the peak/valley values for the missing one(s).
    return(tideData)
if __name__ == '__main__':
  tide = noaaTideData()
  tide.calcTideRange(beginDate = '20110613',
                     endDate = '20110613',
                     station='8661070',
                     datum='MLLW',
                     units='feet',
                     timezone='Local Time',
                     smoothData=False,
                     tideFileDir="C:\\temp")