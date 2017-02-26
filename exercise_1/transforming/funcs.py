#!/usr/bin/env python
# -*- coding: utf-8 -*-
#from pyspark import SparkContext
#from pyspark.sql import HiveContext
#
#from collections import namedtuple
#from collections import *


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
