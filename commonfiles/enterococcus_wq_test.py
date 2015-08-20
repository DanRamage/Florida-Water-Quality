import sys
sys.path.append('../commonfiles')
from math import pow
import logging.config
from sympy.parsing.sympy_parser import parse_expr
from sympy.core.compatibility import exec_
from sympy import Float as symFloat
from sympy import *
from collections import OrderedDict
from wqHistoricalData import wq_defines
from wq_prediction_tests import predictionTest, predictionLevels
import time

class VB_POLY(Function):
  nargs = (1, 4)

  def _eval_evalf(self, nprec):
    obs_symbol,a,b,c = symbols('obs_symbol a b c')
    poly_func = poly(a + b * obs_symbol + c * obs_symbol**2)
    #poly_func.subs({obs_symbol: symFloat(self.args[0]), a: symFloat(self.args[1]), b: symFloat(self.args[2]), c: symFloat(self.args[3])})
    result = poly_func.evalf(subs={obs_symbol: symFloat(self.args[0]), a: symFloat(self.args[1]), b: symFloat(self.args[2]), c: symFloat(self.args[3])})
    return result

class VB_SQUARE(Function):
  nargs = 1

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = obs**2
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result

class VB_QUADROOT(Function):
  nargs = 1

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = obs**.25
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result

class VB_SQUAREROOT(Function):
  nargs = 1

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = obs**.5
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result

class VB_INVERSE(Function):
  nargs = 2

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    if self.args[0] != 0:
      sub_val = symFloat(self.args[0])
    else:
      sub_val = symFloat(self.args[1])
    vb_func =  1 / obs
    result = vb_func.evalf(subs={obs: sub_val})
    return result

class VB_WindO_comp(Function):
  nargs = (1, 3)

  def _eval_evalf(self, nprec):
    wind_dir, wind_spd, beach_orientation = symbols('wind_dir wind_spd beach_orientation')
    vb_func =  wind_spd * sin((wind_dir - beach_orientation) * pi / 180)
    result = vb_func.evalf(subs={wind_dir: symFloat(self.args[0]),
                                 wind_spd: symFloat(self.args[1]),
                                 beach_orientation: symFloat(self.args[2])})
    return result

class VB_WindA_comp(Function):
  nargs = (1, 3)

  def _eval_evalf(self, nprec):
    wind_dir, wind_spd, beach_orientation = symbols('wind_dir wind_spd beach_orientation')
    vb_func =  -wind_spd * cos((wind_dir - beach_orientation) * pi / 180)
    result = vb_func.evalf(subs={wind_dir: symFloat(self.args[0]),
                                 wind_spd: symFloat(self.args[1]),
                                 beach_orientation: symFloat(self.args[2])})
    return result

"""
def VB_POLY(obs_symbol, a_val, b_val, c_val):
  a,b,c = symbols('a b c')
  poly_func = poly(a + b * obs_symbol + c * obs_symbol**2)
  poly_func.subs({a: symFloat(a_val), b: symFloat(b_val), c: symFloat(c_val)})
  return poly_func
"""
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
  def __init__(self, formula, site_name, model_name, use_logger=True):
    predictionTest.__init__(self, formula, site_name)
    self.model_name = model_name
    self.lowCategoryLimit = 104.0
    self.highCategoryLimit = 500.0
    self.mlrResult = None
    self.log10MLRResult = None
    self.logger = None
    self.data_used = OrderedDict()
    self.test_time = 0
    if use_logger:
      self.logger = logging.getLogger(type(self).__name__)


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
  def runTest(self, data):

    if self.logger:
      self.logger.debug("runTest start Site: %s model name: %s formula: %s" % (self.name, self.model_name, self.formula))

    if self.model_name == 'Option A best variables transformed':
      i = 0
    start_time = time.time()
    try:
      #Get the variables from the formula, then verify the passed in data has the observation and a valid value.
      valid_data = True
      sym_expr = sympify(self.formula, globals())

      observation_variables = sym_expr.free_symbols
      mlr_symbols = {}
      for obs_var in observation_variables:
        self.data_used[obs_var.name] = None
        if obs_var.name in data:
          self.data_used[obs_var.name] = data[obs_var.name]
          mlr_symbols[obs_var] = symFloat(data[obs_var.name])
          if data[obs_var.name] == wq_defines.NO_DATA:
            valid_data = False
        else:
          valid_data = False
      if valid_data:
        self.log10MLRResult = sym_expr.evalf(subs=mlr_symbols)
        if self.logger:
          self.logger.debug("Model: %s Result: %f Data Used: %s" % (self.model_name, self.log10MLRResult, self.data_used))
        self.mlrResult = pow(10,self.log10MLRResult)
        self.categorize_result()
      else:
        if self.logger:
          self.logger.debug("Model: %s test not performed, one of more invalid data points: %s" % (self.model_name, self.data_used))
    except Exception,e:
      if self.logger:
        self.logger.exception(e)

    self.test_time = time.time() - start_time
    if self.logger:
      self.logger.debug("Test: %s execute in: %f ms" % (self.model_name, self.test_time * 1000))

      self.logger.debug("runTest finished model: %s Prediction Level: %s" % (self.model_name, self.predictionLevel))

    return self.predictionLevel.value

  """
  Function: mlrCategorize
  Purpose: For the regression formula, this catergorizes the value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def categorize_result(self):
    self.predictionLevel.value = predictionLevels.NO_TEST
    if self.mlrResult is not None:
      if self.mlrResult < self.lowCategoryLimit:
        self.predictionLevel.value = predictionLevels.LOW
      elif self.mlrResult >= self.highCategoryLimit:
        self.predictionLevel.value = predictionLevels.HIGH
      else:
        self.predictionLevel.value = predictionLevels.MEDIUM
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
               name : self.predictionLevel.__str__(),
               'log10MLRResult' : self.log10MLRResult,
               'mlrResult' : self.mlrResult
    }
    return(results)

