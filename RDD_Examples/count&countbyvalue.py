# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('quiz') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/groupby.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x : x.split())

# COMMAND ----------

#count- gives no of values in rdd
rdd2.count()

# COMMAND ----------

#countbyvalue gives each value count
rdd2.countByValue()
