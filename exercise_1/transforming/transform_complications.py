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
# Get complications figures and Z-score them #
#############################################

# Get relevant columns (as strings)
comp_df = sq.sql('SELECT provider_id, \
	measure_id, compared_to_national, denominator, \
	score, lower_estimate, higher_estimate\
	FROM complications \
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
    comp_df = castCol(comp_df, col)

# Compute range of upper - lower estimates
# Could be interesting as a measure of variability
comp_df = comp_df.withColumn('range',
	comp_df['higher_estimate'] - comp_df['lower_estimate'])
cols.append(newCol('range', DoubleType))
comp_df.registerTempTable('comp_stats_tmp')

comp_df = sq.sql('select * from comp_stats_tmp \
	where score is not NULL')

# Get descriptive statistics about columns
#d_mean, d_sd = getDescriptives(comp_df)

tmp_stats_df = sq.sql('select measure_id \
	,avg(score) as mean \
	,stddev_pop(score) as stddev \
	,min(score) as min \
	,max(score) as max \
	from comp_stats_tmp \
	where score is not NULL \
	group by measure_id \
	having mean is not NULL')

scale_max = udf(lambda x: max(100.0, x), DoubleType())

tmp_stats_df = tmp_stats_df.withColumn('scale_min', lit(0.0))

tmp_stats_df = tmp_stats_df.withColumn('scale_max', scale_max('max'))

saveTable(sq, tmp_stats_df, 'comp_stats')


# Join the descriptives to the care df
comp_df = comp_df.alias('a').join(tmp_stats_df.alias('b'), 
	pycol('a.measure_id') == pycol('b.measure_id')).select(
		[pycol('a.'+xx)
		    for xx in comp_df.columns] + 
		[pycol('b.'+xx) for xx in tmp_stats_df.columns
		    if xx != 'measure_id'] )


# All values should be reverse coded (lower is better)
def do_reverse(x):
    return -1.0

comp_df = scale_score_df(sq, comp_df)
comp_df = z_score_df(sq, comp_df, do_reverse)
saveTable(sq, comp_df, 'comp')
saveBest(sq, 'comp')
