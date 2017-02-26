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
effective_care_df = sq.sql('SELECT * FROM effective_care')

# Names and Types to cast columns as
cols = [ newCol('score', DoubleType), ]

# Cast columns
for col in cols:
    effective_care_df = castCol(effective_care_df, col)

# Get descriptive statistics about each measure
saveTable(sq, effective_care_df, 'tmp_df')
tmp_stats_df = sq.sql('select measure_id \
	,avg(score) as mean \
	,stddev_pop(score) as stddev \
	,min(score) as min \
	,max(score) as max \
	from tmp_df \
	where score is not NULL \
	group by measure_id \
	having mean is not NULL')

# Join the descriptives to the care df
effective_care_df = effective_care_df.alias('a').join(tmp_stats_df.alias('b'), 
	pycol('a.measure_id') == pycol('b.measure_id')).select(
		[pycol('a.'+xx)
		    for xx in effective_care_df.columns] + 
		[pycol('b.'+xx) for xx in tmp_stats_df.columns
		    if xx != 'measure_id'] )

#tmp_stats_df = tmp_stats_df.withColumn('reverse', lit(1))

# These are measures where a higher number means worse performance
reverse_coded = [ 'OP_18b', 'OP_20', 'OP_21', 'OP_22', 
	'OP_3b', 'OP_5', 'PC_01', 'VTE_6']

def do_reverse(x):
    return -1.0 if x in reverse_coded else 1.0

# Assign -1 if value should be reverse coded
rev_udf = udf(lambda x: do_reverse(x), DoubleType())
effective_care_df = effective_care_df.withColumn('reverse', rev_udf('measure_id'))

# Compute the z-score for each hospital's measures
cols = [
    newCol('score', DoubleType), 
    newCol('mean', DoubleType), 
    newCol('stddev', DoubleType), 
    newCol('reverse', DoubleType), 
]
for col in cols:
	effective_care_df = castCol(effective_care_df, col)

zscore_udf = udf(lambda x, y, z: z_score(x, y, z), DoubleType())
effective_care_df = effective_care_df.withColumn('z_score', zscore_udf(
    'score', 'mean', 'stddev'))

effective_care_df.registerTempTable('care_tmp')
effective_care_df = sq.sql('select {c_cols}, z_score * reverse as z_score_reversed from care_tmp'.format(c_cols = getColNames(effective_care_df)))
saveTable(sq, effective_care_df, 'care')

best_hospitals = sq.sql('select h.hospital_name, \
sum(e.z_score_reversed) as score, count(e.z_score_reversed) as N \
	from care e, hospitals h \
	where e.provider_id = h.provider_id \
	group by h.hospital_name \
	order by sum(z_score_reversed) DESC')
saveTable(sq, best_hospitals, 'best_hospitals')

best_states = sq.sql('select h.state, \
sum(e.z_score_reversed) as score, count(e.z_score_reversed) as N \
	from care e, hospitals h \
	where e.provider_id = h.provider_id \
	group by h.state \
	order by sum(z_score_reversed) DESC')
saveTable(sq, best_states, 'best_states')

#d = [x.asDict() for x in d]
#dd = dict(( (x['measure_id'], {
#    'mean': x['mean'], 
#    'stddev': x['stddev'],
#    'min': x['min'], 
#    'max': x['max'], 
#    }) for x in d ))

# Z-score the columns
# (x - mu) / sigma
#for col in cols:
#    effective_care_df = effective_care_df.withColumn(
#	    col.name + "_z",
#	    (effective_care_df[col.name] - dd[col.name])/d_sd[col.name])

#saveTable(df = effective_care_df, table = 'effective_care')
#del effective_care_df


