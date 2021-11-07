# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('distinct') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/dataset1.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x : x.split())

# COMMAND ----------

#prints the distinct value in rdd
rdd3 = rdd2.distinct()
rdd3.collect()

# COMMAND ----------

#code proficiency in single line instead of storiung im multiple rdds
rdd.flatMap(lambda x : x.split()).distinct().collect()
