#!bin/env pyspark

#from pyspark.sql.functions import UserDefinedFunction
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import col as pycol
from pyspark.sql.types import StringType, DoubleType, IntegerType

from collections import namedtuple
from collections import *
from funcs import *

# Spark and Hive contexts are automatically loaded in `pyspark`
# but not in `spark-submit`
try: 
    sc = SparkContext()
except: 
    pass
try: 
    sq = HiveContext(sc)
except: 
    sq = sqlContext

# Named tuple to store column info
newCol = namedtuple('newCol', ['name', 'type'])

##########################
# Prepare measures table #
##########################

# Get relevant columns (as strings)
measures_df = sq.sql('select measure_name, measure_id from measures')

saveTable(context = sq, df = measures_df, table = 'measures')
