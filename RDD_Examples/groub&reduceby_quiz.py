# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('quiz') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text = sc.textFile('/FileStore/tables/groupby.txt')

# COMMAND ----------

text.flatMap(lambda x : x.split()).flatMap(lambda x : x).map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y).collect()
