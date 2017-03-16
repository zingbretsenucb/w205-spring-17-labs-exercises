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

######################################################
# Pull just base and consistency scores from surveys #
######################################################

# Get relevant columns (as strings)
surveys_df = sq.sql('SELECT provider_id, \
	base_score, consistency \
	FROM survey_responses')

# Names and Types to cast columns as
cols = [
	newCol('base_score', IntegerType), 
	newCol('consistency', IntegerType), 
	]

# Cast columns to proper types
for col in cols:
    surveys_df = castCol(surveys_df, col)

saveTable(context = sq, df = surveys_df, table = 'surveys')
