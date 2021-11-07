# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('quiz') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text = sc.textFile('/FileStore/tables/groupby.txt')

# COMMAND ----------

rdd2 = text.flatMap(lambda x:x.split())
rdd3 = rdd2.map(lambda x : (x,1))

# COMMAND ----------

#used to get num of partitions assigned
rdd2.getNumPartitions()

# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/output1')
