#!bin/env pyspark

#from pyspark.sql.functions import UserDefinedFunction
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import col as pycol
from pyspark.sql.types import StringType, DoubleType, IntegerType

from collections import namedtuple
from collections import *

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


def getDescriptives(df):
    """Get mean and sd from a df for Z-scoring"""
    d = df.describe().collect()
    d_mean = d[1].asDict()
    d_sd = d[2].asDict()
    return d_mean, d_sd


def saveTable(df, table):
    """Save a df to Hive database"""
    # Register tmp table, then create permanent table
    sq.sql('DROP TABLE tmp')
    df.registerTempTable('tmp')
    sq.sql('DROP TABLE {name}'.format(
	name = table)) # Clear previous
    sq.sql('CREATE TABLE {name} AS SELECT * FROM tmp'.format(
	name = table))
    sq.sql('DROP TABLE tmp') # Clear tmp


def castCol(df, col):
    """Cast column to specific type"""
    return df.withColumn(col.name, df[col.name].cast(col.type()))


def getColNames(df, prefix = ''):
    """Return df column names as string for SQL"""
    return ', '.join(prefix + col for col in df.columns)


def z_score(x, mu, sigma):
    """Return z-score or None type"""
    try: 
	return float((x - mu) / sigma)
    except: 
	return None


def reverse_score(x, y):
    """Reverse z_score, else return None"""
    x = float(x)
    y = float(y)
    return x * y
#    try:
#	return x * r
#    except:
#	return None


#############################################
# Get readmissions figures and Z-score them #
#############################################

# Get relevant columns (as strings)
readmit_df = sq.sql('SELECT provider_id, \
	measure_id, compared_to_national, denominator, \
	score, lower_estimate, higher_estimate\
	FROM readmissions')

# Names and Types to cast columns as
cols = [
	newCol('score', DoubleType), 
	newCol('lower_estimate', DoubleType), 
	newCol('higher_estimate', DoubleType), 
	newCol('denominator', IntegerType), 
	]

# Cast columns
for col in cols:
    readmit_df = castCol(readmit_df, col)
    #readmit_df = readmit_df.withColumn(col.name, 
    #readmit_df[col.name].cast(col.type()))

# Compute range of upper - lower estimates
readmit_df = readmit_df.withColumn('range',
	readmit_df['higher_estimate'] - readmit_df['lower_estimate'])
cols.append(newCol('range', DoubleType))

# Get descriptive statistics about columns
d_mean, d_sd = getDescriptives(readmit_df)

# Z-score the columns
# (x - mu) / sigma
for col in cols:
    readmit_df = readmit_df.withColumn(
	    col.name + "_z",
	    (readmit_df[col.name] - d_mean[col.name])/d_sd[col.name])

saveTable(df = readmit_df, table = 'readmit')
del readmit_df


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
saveTable(effective_care_df, 'tmp_df')
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
	'OP_3b', 'OP_5', 'PC_01']

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
saveTable(effective_care_df, 'care')

# Multiply the z_score by the reverse code value
# So higher numbers are always better
#rev_score_udf = udf(lambda x, y: reverse_score(x, y), DoubleType())
#effective_care_df = effective_care_df.withColumn('z_score_reversed', 
#	rev_score_udf('reverse', 'reverse'))


# measures = sq.sql('select * from measures')

#sq.sql('drop table eff_care')
#sq.sql('create table eff_care as \
#select e.provider_id,  e.conditions, e.measure_id, e.score, \
#t.mean, t.stddev, t.min, t.max, \
#(e.score - t.mean) / t.stddev as z_score, \
#z_score * t.reverse_code as z_score_rev \
#from effective_care e, tmp_stats t \
#where e.measure_id = t.measure_id')


best_hospitals = sq.sql('select h.hospital_name, \
avg(e.z_score_reversed) as score \
	from care e, hospitals h \
	where e.provider_id = h.provider_id \
	group by h.hospital_name \
	order by avg(z_score_reversed) DESC limit 10')
saveTable(best_hospitals, 'best_hospitals')

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

