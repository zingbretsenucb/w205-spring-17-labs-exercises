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

###############################################
# Get effective_care figures and Z-score them #
###############################################

# Get relevant columns (as strings)
effective_care_df = sq.sql('SELECT provider_id, measure_id, \
	score FROM effective_care \
	where score is not NULL')

# Names and Types to cast columns as
cols = [ newCol('score', DoubleType), ]

# Cast columns
for col in cols:
    effective_care_df = castCol(effective_care_df, col)

# Get descriptive statistics about each measure
effective_care_df.registerTempTable('tmp_df')
#saveTable(sq, effective_care_df, 'tmp_df')

# Filter early
effective_care_df = sq.sql('select * from tmp_df \
	where score is not null')

tmp_stats_df = sq.sql('select measure_id \
	,avg(score) as mean \
	,stddev_samp(score) as stddev \
	,min(score) as min \
	,max(score) as max \
	from tmp_df \
	where score is not NULL \
	group by measure_id \
	having mean is not NULL')

scale_max = udf(lambda x: max(100.0, x), DoubleType())

tmp_stats_df = tmp_stats_df.withColumn('scale_min', lit(0.0))

tmp_stats_df = tmp_stats_df.withColumn('scale_max', scale_max('max'))
tmp_stats_cols = [
	newCol('scale_min', DoubleType),
	newCol('scale_max', DoubleType),
	]
for col in tmp_stats_cols:
    tmp_stats_df = castCol(tmp_stats_df, col)
saveTable(sq,tmp_stats_df, 'stats_testing')
#tmp_stats_df = tmp_stats_df.withColumn('score_scaled', scale_score('score', 'scale_min', 'scale_max'))

saveTable(sq, tmp_stats_df, 'timely_stats')

# Join the descriptives to the care df
effective_care_df = effective_care_df.alias('a').join(tmp_stats_df.alias('b'), 
	pycol('a.measure_id') == pycol('b.measure_id')).select(
		[pycol('a.'+xx)
		    for xx in effective_care_df.columns] + 
		[pycol('b.'+xx) for xx in tmp_stats_df.columns
		    if xx != 'measure_id'] )
#effective_care_df.registerTempTable('tmp')
#effective_care_df = sq.sql('select * from tmp where score is not NULL')

#tmp_stats_df = tmp_stats_df.withColumn('reverse', lit(1))

# These are measures where a higher number means worse performance
reverse_coded = ['OP_1', 'OP_18b', 'OP_20', 'OP_21', 'OP_22', 
	'OP_3b', 'OP_5', 'PC_01', 'VTE_6',
	'ED_1b', 'ED_2B']

def do_reverse(x):
    return -1.0 if x in reverse_coded else 1.0

if True:    
    cols.append(newCol('scale_min', DoubleType))
    cols.append(newCol('scale_max', DoubleType))
    for col in cols:
	effective_care_df = castCol(effective_care_df, col)

    # This will scale the score between 0 and 1 so we can compare variability across themeasures
    effective_care_df = scale_score_df(sq, effective_care_df)

# This z-scores the measures so we can compare performance across hospitals
if True:
    effective_care_df = z_score_df(sq, effective_care_df, do_reverse)

saveTable(sq, effective_care_df, 'timely')
saveBest(sq, 'timely')
