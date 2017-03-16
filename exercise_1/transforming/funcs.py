#!/usr/bin/env python
# -*- coding: utf-8 -*-
#from pyspark import SparkContext
#from pyspark.sql import HiveContext
#
#from collections import namedtuple
#from collections import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, IntegerType

from collections import namedtuple
newCol = namedtuple('newCol', ['name', 'type'])


def getDescriptives(df):
    """Get mean and sd from a df for Z-scoring"""
    d = df.describe().collect()
    d_mean = d[1].asDict()
    d_sd = d[2].asDict()
    return d_mean, d_sd


def saveTable(context, df, table):
    """Save a df to Hive database"""
    # Register tmp table, then create permanent table
    context.sql('DROP TABLE tmp')
    df.registerTempTable('tmp')
    context.sql('DROP TABLE {name}'.format(
	name = table)) # Clear previous
    context.sql('CREATE TABLE {name} AS SELECT * FROM tmp'.format(
	name = table))
    context.sql('DROP TABLE tmp') # Clear tmp


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

def scale_score(x, lb, ub):
    """Return scaled score or None type"""
    try: 
	return float((x - lb) / ub)
    except: 
	return None


def reverse_score(x, y):
    """Reverse z_score, else return None"""
    x = float(x)
    y = float(y)
    return x * y


def scale_score_df(context, df, prefix = ""):
    """Takes a df with `score`, `scale_min`, and `scale_max` columns"""

    score = prefix + 'score'
    scale_min = prefix + 'scale_min'
    scale_max = prefix + 'scale_max'

    # Compute the scaled score for each hospital's measures
    cols = [
	    newCol(score, DoubleType), 
	    newCol(scale_min, DoubleType), 
	    newCol(scale_max, DoubleType), 
	    ]
    for col in cols:
	df = castCol(df, col)

    scale_score_udf = udf(lambda x, lb, ub: scale_score(x, lb, ub), DoubleType())
    df = df.withColumn(score + '_scaled', scale_score_udf(
	score, scale_min, scale_max))

    return df


def z_score_df(context, df, reverse_func, prefix = ""):
    """Takes a df with `score`, `mean`, and `stddev` columns"""

    score = prefix + 'score'
    z = prefix + 'z_score'
    mu = prefix + 'mean'
    sigma = prefix + 'stddev'
    rev = prefix + 'reverse'

    # Assign -1 if value should be reverse coded
    rev_udf = udf(lambda x: reverse_func(x), DoubleType())
    df = df.withColumn(rev, rev_udf('measure_id'))

    # Compute the z-score for each hospital's measures
    cols = [
	    newCol(score, DoubleType), 
	    newCol(mu, DoubleType), 
	    newCol(sigma, DoubleType), 
	    newCol(rev, DoubleType), 
	    ]
    for col in cols:
	df = castCol(df, col)

    zscore_udf = udf(lambda x, y, z: z_score(x, y, z), DoubleType())
    df = df.withColumn(z, zscore_udf(
	score, mu, sigma))

    df.registerTempTable('tmp')
    df = context.sql('select {colNames}, {z} * {rev} as {z}_reversed from tmp'.format(
	colNames = getColNames(df),
	z = z,
	rev = rev))
    return df

# What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.
# What states are models of high-quality care?
def saveBest(context, table_from, threshold = 5):
    """Rank hospitals and states by average of z-scores"""
    best_hospitals = context.sql('select h.provider_id, \
	    h.hospital_name, sum(e.z_score_reversed) as score, \
	    count(e.z_score_reversed) as n \
	    from {table_from} e, hospitals h \
	    where e.provider_id = h.provider_id \
	    group by h.provider_id, h.hospital_name \
	    order by score DESC'.format(
		table_from = table_from,
		)
	    )
	    #having n >= {threshold} \
		#threshold = str(threshold)

    saveTable(context, best_hospitals, 'best_hospitals_' + table_from)

    # States have enough data points to average
    best_states = context.sql('select h.state, \
	    avg(e.z_score_reversed) as score, count(e.z_score_reversed) as N \
	    from {table_from} e, hospitals h \
	    where e.provider_id = h.provider_id \
	    group by h.state \
	    order by avg(z_score_reversed) DESC'.format(
		table_from = table_from)
	    )
    
    saveTable(context, best_states, 'best_states_' + table_from)
    return best_hospitals, best_states


# Which procedures have the greatest variability between hospitals?
def calculateVariability(context, table_from):
    tmp_stats_df = sq.sql('select measure_id \
	,avg(score) as mean \
	,stddev_samp(score) as stddev \
	,min(score) as min \
	,max(score) as max \
	from {table_from} \
	where score is not NULL \
	group by measure_id \
	having mean is not NULL'.format(
	    table_from = table_from))
    
# Are average scores for hospital quality or procedural variability correlated with patient survey responses?
def calculateCorrelation(context, table1, table2):
    pass
