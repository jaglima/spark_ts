from pyspark.sql.dataframe import DataFrame
from .spark_ts import *


 



# Patching the DataFrame object to implements transform functionality
def transform(self, f):
    return f(self)

DataFrame.transform = transform

