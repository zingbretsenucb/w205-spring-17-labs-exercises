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

##############################################
# Combine procedures and readmittance scores #
##############################################

# Get relevant columns (as strings)
all_measure_scores = sq.sql('SELECT provider_id, measure_id, \
	z_score_reversed as score, score_scaled FROM timely \
	UNION ALL \
	SELECT provider_id, measure_id, \
	z_score_reversed, score_scaled FROM readmit')

saveTable(sq, all_measure_scores, 'all_measures')
