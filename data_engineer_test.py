# -*- coding: utf-8 -*-
## ==============================================================
## GENERAL IMPORTS
## ==============================================================
import os
import sys
import pyspark
import re
import json
import time
from datetime import datetime as dtime
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *


## ==============================================================
## GENERAL PATHS | SPARK ENV
## ==============================================================

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)


path_ga_data = os.path.join("data", "data.gz")


## ==============================================================
## DATA ENGINEERING TEST
## ==============================================================

try:
    # df_ga_data = sqlContext.read.option("encoding", "UTF-8").json(path_ga_data)
    df_ga_data = spark.read.option("encoding", "UTF-8").json(path_ga_data)
    df_ga_data.unpersist()
    df_ga_data.cache()
except:
    print("No file found under path '{0}'...".format(path_ga_data))
    sys.exit(1)


## 1 - Pageviews count
## -- -----------------------------------
print("Contagem de pageviews...")
df_ga_data.selectExpr("totals.pageviews AS pageviews").agg(sum("pageviews").alias("pageviews_count")).show(1, False)


## 2 - Session count by user
## -- -----------------------------------
print("Quantidade de sessoes por usuario (TOP 10)...")
df_ga_data.groupBy(expr("CONCAT(fullVisitorId, visitId) AS unique_user_id")) \
            .count() \
            .select("unique_user_id", col('count').alias("session_qtty")) \
            .sort("session_qtty", ascending = False) \
            .show(10, False)



## 3 - Distinct Sessions per Date
## -- -----------------------------------
print("Quantidade de sessoes distintas por data (TOP 10)...")
df_ga_data.groupBy("date") \
            .agg(countDistinct(expr("CONCAT(fullVisitorId, visitId)")).alias("session_qtty")) \
            .select("date", "session_qtty") \
            .sort("session_qtty", ascending = False) \
            .show(10, False)



## 4 - Average session time per Date
## -- -----------------------------------
print("Media de duracao da sessao (em segundos) por data (ordenado por menor data, 10 primeiras linhas)...")
df_ga_data.groupBy("date") \
            .agg(avg("totals.timeOnSite").alias("session_avg_time")) \
            .select("date", "session_avg_time") \
            .sort("date", ascending = True) \
            .show(10, False)            




## 5 - Session quantity per day and device
## -- -----------------------------------
print("Quantidade de sessoes por dia e device (ordenado por menor data, 30 primeiras linhas)...")
df_ga_data.groupBy("date", "device.browser") \
            .agg(countDistinct(expr("CONCAT(fullVisitorId, visitId)")).alias("session_qtty")) \
            .selectExpr("date", "browser", "session_qtty") \
            .sort("date", ascending = True) \
            .show(30, False)