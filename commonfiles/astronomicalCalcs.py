import sys
import math
"""
  All calculations taken from Astronomical Formulas for Calculators.
"""
class julianDate(object):
  def __init__(self, dateTime):
    self.julianTime    = self.calcJulianDate(dateTime)
    self.julianCentury = self.calcJulianCenturies(self.julianTime)

  def calcJulianDate(self, dateTime):
    import time
    convertTime = time.strptime(dateTime, "%Y-%m-%d %H:%M:%S")
    timeFrac = (convertTime[3] + (convertTime[4] / 60.0) + ((convertTime[5] / 60.0) / 60.0)) / 24
    #Figure out if we are using Gregorian calender.
    #Year.MMDDdd
    year = convertTime[0]
    month = convertTime[1]
    if(month == 1 or month == 2):
      year -= 1
      month += 12
    tstTime = year + ( month + ((convertTime[2] + timeFrac)/100.0))/100.0
    B=0
    if(tstTime > 1582.1015):
      #A = int(y/100)
      #B = 2 - A + int(A/4)
      A = int(year / 100)
      B = 2 - A + int(A/4)    
    #JD = INT (365.25y) + INT (30.6001 (m + 1)) + DD.dd + 1720 994.5 
    julDate = int(365.25 * year) + int(30.6001 * ( month + 1 )) + (convertTime[2] + timeFrac) + 1720994.5 + B
    return(julDate)

  def calcJulianCenturies(self, julianDate):
    T = (julianDate - 2415020.0) / 36525.0
    return(T)  
 
class astronomicalCalcs(julianDate):
  def __init__(self, dateTime):
    julianDate.__init__(self,dateTime)    
    self.dateTime     = dateTime    
    
  def setDate(self, dateTime):
    self.dateTime     = dateTime
    self.calcJulianDate(self.dateTime)
    self.calcJulianCenturies(self.julianDate)
    
    
 
  """
    The equation of Kepler is 
    E = M + e sin E 
    where e is the eccentricity of the planet's orbit, M the planet's 
    mean anomaly at a given instant, and E the eccentric anomaly. Ge- 
    nerally, e and M are given, and the equation must be solved for E, 
    as in Chapters 18, 25 and 39. The eccentric anomaly E is an auxi- 
    liary quantity which is needed to find the true anomaly v. 
    Equation (22.1) is a transcendental equation in E and cannot 
    be solved directly. We will describe two iteration methods for 
    finding E (iteration = repetition), and finally a formula which 
    gives an approximate result.   
  """  
  def calcKepler(self, eccentricityOfOrbit, meanAnomaly, maxIterations=1000, desiredCorrection=0.000000001 ):
    import math

    iterationCnt = 0
    E1 = 0
    M = meanAnomaly
    E0 = meanAnomaly
    e0 = math.degrees(eccentricityOfOrbit)
    e = eccentricityOfOrbit    
    kepler = None
    #Go through no more than maxIterations to determine Eccentric anomaly.
    while(iterationCnt < maxIterations):
      #Initial E0 is value for the mean anomaly, M. 
      #e0 is the radians value for the orbit eccentricity.
      #e is the orbit eccentricity in degrees.
      E0Rads = math.radians(E0)
      E1 = E0 + ((M + e0 * math.sin(E0Rads) - E0) / (1 - e * math.cos(E0Rads)))
      curCorrection = math.fabs(E1 - E0)
      E0 = E1                      
      iterationCnt += 1
      if(curCorrection <= desiredCorrection):
        kepler = E1
        break
    return(kepler)
          
  def calcEarthOrbitEccentricity(self):
    return(0.01675104 - (0.0000418 * self.julianCentury) - (0.000000126 * (self.julianCentury*self.julianCentury))) 

  def convert360(self, value):
    degrees = value - math.floor(value/360.0)*360 ;
    return(degrees)
    

class moon(astronomicalCalcs):
  def __init__(self, dateTime):
    astronomicalCalcs.__init__(self,dateTime)

  def doCalcs(self):
    #Create the sun data, we need the mean anomaly and the true longitude for some moon calcs.
    sunCalcs = sun(self.dateTime)
    sunCalcs.doCalcs()
    #Mean longitude(Variable name in documentation L')
    self.meanLongitude = 270.434164 \
                         + (481267.8831 * self.julianCentury) \
                         - (0.001133 * self.julianCentury * self.julianCentury) \
                         + (0.0000019 * self.julianCentury * self.julianCentury * self.julianCentury )
    self.meanLongitude = self.convert360(self.meanLongitude)                         
                               
    #Moons mean anomaly(Variable name in documentation M')                        
    self.meanAnomaly = 296.104608 \
                          + (477198.8491 * self.julianCentury) \
                          + (0.009192 * self.julianCentury * self.julianCentury) \
                          + (0.0000144 * self.julianCentury * self.julianCentury * self.julianCentury)
    self.meanAnomaly = self.convert360(self.meanAnomaly)
                              
    #Moon's mean elongation(Variable name in documentation D)                      
    self.meanElongation = 350.737486 \
                              + (445267.1142 * self.julianCentury)\
                              - (0.001436 * self.julianCentury * self.julianCentury) \
                              + (0.0000019 * self.julianCentury * self.julianCentury * self.julianCentury)
    self.meanElongation = self.convert360(self.meanElongation)
                                   
    #Mean distance of Moon from its ascending node(Variable name in documentation F)                         
    self.distToAscNode = 11.250889\
                             + (483202.0251 * self.julianCentury)\
                             - (0.003211 * self.julianCentury * self.julianCentury)\
                             - (0.0000003 * self.julianCentury * self.julianCentury * self.julianCentury)
    self.distToAscNode = self.convert360(self.distToAscNode)
     
    #Longitude of moon's ascending node(Variable name in documentation Omega)
    self.ascNodeLong = 259.183275\
                       - (1934.1420 * self.julianCentury) \
                       + (0.002078 * self.julianCentury * self.julianCentury)\
                       + (0.0000022 * self.julianCentury * self.julianCentury * self.julianCentury)
    self.ascNodeLong = self.convert360(self.ascNodeLong)
                               
    #Now add in the "additives"
    #The first four terms have a period of 1782 years. The fifth term, 
    #with coefficient 0?003 964, is the" Great Venus Term"; its period 
    #is 271 years. 
    
    self.meanLongitude += (0.000233 * math.sin(math.radians(51.2 + 20.2 * self.julianCentury))) \
                      + (0.003964 * math.sin(math.radians(346.560 + 132.870 * self.julianCentury - 0.0091731 * (self.julianCentury*self.julianCentury))))\
                      + (0.001964 * math.sin(math.radians(self.ascNodeLong))) 
    self.meanLongitude = self.convert360(self.meanLongitude)
    
    sunMeanAnomaly = sunCalcs.meanAnomaly
    sunMeanAnomaly -= (0.001778 * math.sin(math.radians(51.2 + 20.2 * self.julianCentury)))
    
    self.meanAnomaly += (0.000817 * math.sin(math.radians(51.2 + 20.2 * self.julianCentury)))\
                      + (0.003964 * math.sin(math.radians(346.560 + 132.870 * self.julianCentury - 0.0091731 * (self.julianCentury*self.julianCentury))))\
                      + (0.002541 * math.sin(math.radians(self.ascNodeLong)))
    self.meanAnomaly = self.convert360(self.meanAnomaly)
                          
    self.meanElongation += (0.002011 * math.sin(math.radians(51.2 + 20.2 * self.julianCentury))\
                        + (0.003964 * math.sin(math.radians(346.560 + 132.870 * self.julianCentury - 0.0091731 * (self.julianCentury*self.julianCentury))))\
                        + 0.001964 * math.sin(math.radians(self.ascNodeLong)))
    self.meanElongation = self.convert360(self.meanElongation)
     
    self.distToAscNode += (0.003964 * math.sin(math.radians(346.560 + 132.870 * self.julianCentury - 0.0091731 * (self.julianCentury*self.julianCentury))))\
                       - (0.024691 * math.sin(math.radians(self.ascNodeLong)))\
                       - (0.004328 * math.sin(math.radians(self.ascNodeLong + 275.05 - 2.30 * self.julianCentury)))
    self.distToAscNode = self.convert360(self.distToAscNode)                    
         
    F = math.radians(self.distToAscNode)
    M1 = math.radians(self.meanAnomaly)
    D = math.radians(self.meanElongation)
    M = math.radians(sunMeanAnomaly)
    EX = 1 - (.002495 * self.julianCentury) - (.00000752 * self.julianCentury * self.julianCentury)
    #Calculate the longitude and latitude using the periodic terms. 
    self.longitude = self.meanLongitude + 6.28875 * math.sin(M1)\
      + 1.274018 * math.sin(2 * D - M1)\
      + .658309 * math.sin(2 * D)\
      + .213616 * math.sin(2 * M1)\
      - EX * .185596 * math.sin(M)\
      - .114336 * math.sin(2 * F)\
      + .058793 * math.sin(2 * D - 2 * M1)\
      + EX * .057212 * math.sin(2 * D - M - M1)\
      + .05332 * math.sin(2 * D + M1)\
      + EX * .045874 * math.sin(2 * D - M)\
      + EX * .041024 * math.sin(M1 - M)\
      - .034718 * math.sin(D)\
      - EX * .030465 * math.sin(M + M1)\
      + .015326 * math.sin(2 * D - 2 * F)\
      - .012528 * math.sin(2 * F + M1)\
      - .01098 * math.sin(2 * F - M1)\
      + .010674 * math.sin(4 * D - M1)\
      + .010034 * math.sin(3 * M1)\
      + .008548 * math.sin(4 * D - 2 * M1)\
      - EX * .00791 * math.sin(M - M1 + 2 * D)\
      - EX * .006783 * math.sin(2 * D + M)\
      + .005162 * math.sin(M1 - D)\
      + EX * .005 * math.sin(M + D)\
      + EX * .004049 * math.sin(M1 - M + 2 * D)\
      + .003996 * math.sin(2 * M1 + 2 * D)\
      + .003862 * math.sin(4 * D)\
      + .003665 * math.sin(2 * D - 3 * M1)\
      + EX * .002695 * math.sin(2 * M1 - M)\
      + .002602 * math.sin(M1 - 2 * F - 2 * D)\
      + EX * .002396 * math.sin(2 * D - M - 2 * M1)\
      - .002349 * math.sin(M1 + D)\
      + EX * EX * .002249 * math.sin(2 * D - 2 * M)\
      - EX * .002125 * math.sin(2 * M1 + M)\
      - EX * EX * .002079 * math.sin(2 * M)\
      + EX * EX * .002059 * math.sin(2 * D - M1 - 2 * M)\
      - .001773 * math.sin(M1 + 2 * D - 2 * F)\
      + EX * .00122 * math.sin(4 * D - M - M1)\
      - .00111 * math.sin(2 * M1 + 2 * F)\
      + .000892 * math.sin(M1 - 3 * D)\
      - EX * .000811 * math.sin(M + M1 + 2 * D)\
      + EX * .000761 * math.sin(4 * D - M - 2 * M1)\
       + EX * EX*.000717 * math.sin(M1 - 2 * M)\
      + EX * EX * .000704 * math.sin(M1 - 2 * M - 2 * D)\
      + EX * .000693 * math.sin(M - 2 * M1 + 2 * D)\
      + EX * .000598 * math.sin(2 * D - M - 2 * F)\
      + .00055 * math.sin(M1 + 4 * D)\
      + .000538 * math.sin(4 * M1)\
      + EX * .000521 * math.sin(4 * D - M)\
      + .000486 * math.sin(2 * M1 - D)\
      - .001595 * math.sin(2 * F + 2 * D)  
    
    
    
    self.latitude = 5.128189 * math.sin(F)\
      + .280606 * math.sin(M1 + F)\
      + .277693 * math.sin(M1 - F)\
      + .173238 * math.sin(2 * D - F)\
      + .055413 * math.sin(2 * D + F - M1)\
      + .046272 * math.sin(2 * D - F - M1)\
      + .032573 * math.sin(2 * D + F)\
      + .017198 * math.sin(2 * M1 + F)\
      + 9.266999E-03 * math.sin(2 * D + M1 - F)\
      + .008823 * math.sin(2 * M1 - F)\
      + EX * .008247 * math.sin(2 * D - M - F)\
      + .004323 * math.sin(2 * D - F - 2 * M1)\
      + .0042 * math.sin(2 * D + F + M1)\
      + EX * .003372 * math.sin(F - M - 2 * D)\
      + EX * .002472 * math.sin(2 * D + F - M - M1)\
      + EX * .002222 * math.sin(2 * D + F - M)\
      + .002072 * math.sin(2 * D - F - M - M1)\
      + EX * .001877 * math.sin(F - M + M1)\
      + .001828 * math.sin(4 * D - F - M1)\
      - EX * .001803 * math.sin(F + M)\
      - .00175 * math.sin(3 * F)\
      + EX * .00157 * math.sin(M1 - M - F)\
      - .001487 * math.sin(F + D)\
      - EX * .001481 * math.sin(F + M + M1)\
      + EX * .001417 * math.sin(F - M - M1)\
      + EX * .00135 * math.sin(F - M)\
      + .00133 * math.sin(F - D)\
      + .001106 * math.sin(F + 3 * M1)\
      + .00102 * math.sin(4 * D - F)\
      + .000833 * math.sin(F + 4 * D - M1)\
      + .000781 * math.sin(M1 - 3 * F)\
      + .00067 * math.sin(F + 4 * D - 2 * M1)\
      + .000606 * math.sin(2 * D - 3 * F)\
      + .000597 * math.sin(2 * D + 2 * M1 - F)\
      + EX * .000492 * math.sin(2 * D + M1 - M - F)\
      + .00045 * math.sin(2 * M1 - F - 2 * D)\
      + .000439 * math.sin(3 * M1 - F)\
      + .000423 * math.sin(F + 2 * D + 2 * M1)\
      + .000422 * math.sin(2 * D - F - 3 * M1)\
      - EX * .000367 * math.sin(M + F + 2 * D - M1)\
      - EX * .000353 * math.sin(M + F + 2 * D)\
      + .000331 * math.sin(F + 4 * D)\
      + EX * .000317 * math.sin(2 * D + F - M + M1)\
      + EX * EX * .000306 * math.sin(2 * D - 2 * M - F)\
      - .000283 * math.sin(M1 + 3 * F)
      
    #W1 = .0004664 * math.cos(math.radians(self.ascNodeLong))
    #W2 = .0000754 * math.cos(math.radians((self.ascNodeLong + 275.05 - 2.3 * self.julianCentury)))
    #BT = B * (1 - W1 - W2) 
    
    d = math.cos(math.radians(self.longitude - sunCalcs.trueLongitude)) * math.cos(math.radians(self.latitude))
    d = math.degrees(math.acos(d)) 
        
    self.moonPhaseAngle = 180 - d - ( 0.1468 * ((1 - 0.0549 * math.sin(M1))/(1 - 0.0167 * math.sin(M))) * math.sin(math.radians(d)))                            

    """
    The illuminated fraction k of the Moon's disk, as seen from the 
    center of the Earth, can be calculated from 
    k = (1 + cos(i)/ 2) 
    where i is the Moon's phase angle, that is the angular distance 
    Sun - Earth as seen from the Moon. 
    """
    self.percentIllumination = (1 + math.cos(math.radians(self.moonPhaseAngle))) / 2
             
    return
  
    
class sun(astronomicalCalcs):
  def __init__(self, dateTime):
    astronomicalCalcs.__init__(self,dateTime)
    self.meanAnomaly   = None #M
    self.earthOrbitEcc    = None #e
    self.kepler           = None #E
    self.trueAnomaly   = None #v
    self.meanLongitude = None #L     
    self.trueLongitude = None #Sigma
    self.center        = None #C
    
  def doCalcs(self):
    #Calculate mean anomaly
    M = 358.47583 + (35999.04975 * self.julianCentury)- (0.000150 * (self.julianCentury*self.julianCentury)) - (0.0000033 * (self.julianCentury*self.julianCentury*self.julianCentury))
    self.meanAnomaly = self.convert360(M) 

    self.earthOrbitEcc    = self.calcEarthOrbitEccentricity()
    self.kepler           = self.calcKepler(self.earthOrbitEcc, self.meanAnomaly)
    """
    Sun's equation of center C as follows:

    C = (1.9l9460 - 0.004789*T - O.OOO014*T*T) sin M 
    + (O.020094 - O.OOO100*T) * sin(2*M) 
    + O.OOO293*sin(3*M) 
    """
    M = math.radians(self.meanAnomaly)
    self.center = ((1.919460 - (0.004789 * self.julianCentury) - (0.000014 * self.julianCentury * 2)) * math.sin(M)) \
      + ((0.020094 - 0.000100 * self.julianCentury) * math.sin(2*M)) \
      + (0.000293 * math.sin(3*M)) 
    
    #Calculate longitudes
    L0 = 279.69668 + (36000.76892 * self.julianCentury) + (0.0003025 * (self.julianCentury*self.julianCentury))
    #Convert value to 360degree reference.
    self.meanLongitude = self.convert360(L0) 
    self.trueLongitude = self.meanLongitude + self.center

if __name__ == '__main__':

  import time
  
  moonCalcs = moon('2000-01-01 17:00:00')
  moonCalcs.doCalcs()
  
  sys.exit(0)