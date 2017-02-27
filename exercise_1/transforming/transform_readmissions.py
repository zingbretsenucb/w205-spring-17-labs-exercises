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

#############################################
# Get readmissions figures and Z-score them #
#############################################

# Get relevant columns (as strings)
readmit_df = sq.sql('SELECT provider_id, \
	measure_id, score, lower_estimate, higher_estimate\
	FROM readmissions \
	where score is not NULL')

# Names and Types to cast columns as
cols = [
	newCol('score', DoubleType), 
	newCol('lower_estimate', DoubleType), 
	newCol('higher_estimate', DoubleType), 
	newCol('denominator', IntegerType), 
	]

# Cast columns to proper types
for col in cols:
    readmit_df = castCol(readmit_df, col)

# Compute range of upper - lower estimates
# Could be interesting as a measure of variability
readmit_df = readmit_df.withColumn('range',
	readmit_df['higher_estimate'] - readmit_df['lower_estimate'])
cols.append(newCol('range', DoubleType))
readmit_df.registerTempTable('readmit_stats_tmp')

readmit_df = sq.sql('select * from readmit_stats_tmp \
	where score is not NULL')

# Get descriptive statistics about columns
#d_mean, d_sd = getDescriptives(readmit_df)

tmp_stats_df = sq.sql('select measure_id \
	,avg(score) as mean \
	,stddev_pop(score) as stddev \
	,min(score) as min \
	,max(score) as max \
	from readmit_stats_tmp \
	where score is not NULL \
	group by measure_id \
	having mean is not NULL')

scale_max = udf(lambda x: max(100.0, x), DoubleType())

tmp_stats_df = tmp_stats_df.withColumn('scale_min', lit(0.0))

tmp_stats_df = tmp_stats_df.withColumn('scale_max', scale_max('max'))

saveTable(sq, tmp_stats_df, 'readmit_stats')


# Join the descriptives to the care df
readmit_df = readmit_df.alias('a').join(tmp_stats_df.alias('b'), 
	pycol('a.measure_id') == pycol('b.measure_id')).select(
		[pycol('a.'+xx)
		    for xx in readmit_df.columns] + 
		[pycol('b.'+xx) for xx in tmp_stats_df.columns
		    if xx != 'measure_id'] )


# All values should be reverse coded (lower is better)
def do_reverse(x):
    return -1.0

readmit_df = scale_score_df(sq, readmit_df)
readmit_df = z_score_df(sq, readmit_df, do_reverse)
saveTable(sq, readmit_df, 'readmit')

readmit_df = sq.sql('select provider_id, measure_id, \
	score_scaled, z_score_reversed from readmit')
saveTable(sq, readmit_df, 'readmit')
saveBest(sq, 'readmit')
