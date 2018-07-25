# This code is part of the Spark_ts distribution and governed by its
# license.  Please see the LICENSE file that should have been included
# as part of this package.
"""Collection of modules for dealing with time series data in Pyspark.
"""
import warnings
from pyspark.sql.dataframe import DataFrame
from .spark_ts import *


__version__ = "0.1.0"


class spark_ts_warning(Warning):
    def __init__(self):
        warnings.warn(
	    """spark_ts warning.
	    	Spark_ts is experimental code at the current level of development.
	    	"""
	)    
    pass



""" 
Patching the DataFrame object adding transform functionality to Pyspark 
Dataframe
"""
def transform(self, f):

    return f(self)

DataFrame.transform = transform

