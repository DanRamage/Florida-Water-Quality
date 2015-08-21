import math

import logging.config

class predictionLevels(object):
  NO_TEST = -1
  LOW = 1
  MEDIUM = 2
  HIGH = 3
  def __init__(self, value):
    self.value = value
  def __str__(self):
    if self.value == self.LOW:
      return "LOW"
    elif self.value == self.MEDIUM:
      return "MEDIUM"
    elif self.value == self.HIGH:
      return "HIGH"
    else:
      return "NO TEST"

"""
Class: predictionTest
Purpose: This is the base class for our various prediction tests.
"""
class predictionTest(object):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters:
    formula - a string with the appropriate string substitution parameters that the runTest function will
      apply the data against.
    name - A string identifier for the test.
  Return:
  """
  def __init__(self, formula, name=None):
    self.formula = formula
    self.predictionLevel = predictionLevels(predictionLevels.NO_TEST)
    self.name = name
    self.test_time = None

  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
  Parameters:
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def runTest(self, data):
    return predictionLevels.NO_TEST

  """
  Function: getResults
  Purpose: Returns a dictionary with the computational variables that went into the predictionLevel. For instance, for an
    MLR calculation, there are intermediate results such as the log10 result and the final result.
  Parameters:
    None
  Return: A dictionary.
  """
  def getResults(self):
    results = {'predictionLevel' : self.predictionLevel.__str__()}
    return results



"""
Class wqTest
Purpose: This is the base class for the actually water quality prediction process.
 Each watershed area has its own MLR and CART tests, so this base class doesn't implement
 anything other than stub functions for them.
"""
class wqEquations(object):
  """
  Function: __init__
  Purpose: Initializes the object with all the tests to be performed for the station.
  Parameters:
    station - The name of the station this object is being setup for.
    model_equation_list - List of model test objects for the site.
    logger - A reference to the logging object to use.
  """
  def __init__(self, station, model_equation_list, use_logger=True):
    self.station = station  #The station that this object represents.
    self.tests = []
    self.ensemblePrediction = predictionLevels.NO_TEST
    for model_equation in model_equation_list:
      self.tests.append(model_equation)
    self.data = {} #Data used for the tests.

    self.logger = None
    if use_logger:
      self.logger = logging.getLogger(type(self).__name__)

  """
  Function: addTest
  Purpose: Adds a prediction test to the list of tests.
  Parameters:
    predictionTestObj -  A predictionTest object to use for testing.
  """
  def addTest(self, predictionTestObj):
    self.tests.append(predictionTestObj)

  """
  Function: runTests
  Purpose: Runs the suite of tests, current a regression formula and CART model, then tabulates
    the overall prediction.
  Parameters:
    dataDict - A data dictionary keyed on the variable names in the CART tree. String subsitution
      is done then the formula is evaled.
  Return:
    A predictionLevels value representing the overall prediction level. This is the average of the individual
    prediction levels.
  """
  def runTests(self, test_data):
    self.data = test_data.copy()

    for testObj in self.tests:
      testObj.runTest(test_data)

    self.overallPrediction()
  """
  Function: overallPrediction
  Purpose: From the models used, averages their predicition values to come up with the overall value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def overallPrediction(self):
    allTestsComplete = True
    executedTstCnt = 0
    if len(self.tests):
      sum = 0
      for testObj in self.tests:
        #DWR 2011-10-11
        #If a test wasn't executed, we skip using it.
        if testObj.predictionLevel != predictionLevels.NO_TEST:
          sum += testObj.predictionLevel.value
          executedTstCnt += 1

      if executedTstCnt:
        self.ensemblePrediction = int(round(sum / float(executedTstCnt)))


    if self.logger is not None:
      self.logger.debug("Overall Prediction: %d(%s)" %(self.ensemblePrediction, predictionLevels(self.ensemblePrediction).__str__()))
    return self.ensemblePrediction


