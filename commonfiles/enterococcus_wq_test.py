import sys
sys.path.append('../commonfiles')
from math import pow

from sympy.parsing.sympy_parser import parse_expr
from sympy import *

from wq_prediction_tests import predictionTest, predictionLevels
"""
Class: mlrPredictionTest
Purpose: Prediction test for a linear regression formula.
"""
class EnterococcusPredictionTest(predictionTest):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters:
  formula - a string with the appropriate string substitution parameters that the runTest function will
    apply the data against.
  lowCategoryLimit - A float that defines the lower limit which categorizes the test result as a LOW probability.
  highCategoryLimit - A float that defines the high limit which categorizes the test result as a HIGH probability.
  Return:
  """
  def __init__(self, formula, site_name, model_name):
    predictionTest.__init__(self, formula, site_name)
    self.model_name = model_name
    self.lowCategoryLimit = 104.0
    self.highCategoryLimit = 500.0
    self.mlrResult = None
    self.log10MLRResult = None


  """
  Function: setCategoryLimits
  Purpose: To catecorize MLR results, we use a high and low limit.
  Parameters:
    lowLimit - Float representing the value, equal to or below, which is considered a low prediction.
    highLimit  - Float representing the value, greater than,  which is considered a high prediction.
  """
  def set_category_limits(self, lowLimit, highLimit):
    self.lowCategoryLimit = lowLimit
    self.highCategoryLimit = highLimit
  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
    Prediction is a log10 formula.
  Parameters:
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def run_test(self, data):
    #Use the formula and the data dictionary to solve our linear equation.
    if self.logger:
      self.logger.debug("Site: %s model name: %s formula: %s" % (self.name, self.model_name, self.formula))

    self.log10MLRResult = parsed_expr(self.formula, data)
    self.mlrResult = pow(10,self.log10MLRResult)
    self.mlrCategorize()
    return self.predictionLevel

  """
  Function: mlrCategorize
  Purpose: For the regression formula, this catergorizes the value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def categorize_result(self):
    self.predictionLevel = predictionLevels.NO_TEST
    if self.mlrResult is not None:
      if self.mlrResult < self.lowCategoryLimit:
        self.predictionLevel = predictionLevels.LOW
      elif self.mlrResult >= self.highCategoryLimit:
        self.predictionLevel = predictionLevels.HIGH
      else:
        self.predictionLevel = predictionLevels.MEDIUM
  """
  Function: getResults
  Purpose: Returns a dictionary with the variables that went into the predictionLevel.
  Parameters:
    None
  Return: A dictionary.
  """
  def get_result(self):
    name = "%s_%s_Prediction" % (self.name, self.model_name)
    results = {
               name : predictionLevels(self.predictionLevel).__str__(),
               'log10MLRResult' : self.log10MLRResult,
               'mlrResult' : self.mlrResult
    }
    return(results)

