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


def reverse_score(x, y):
    """Reverse z_score, else return None"""
    x = float(x)
    y = float(y)
    return x * y


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
