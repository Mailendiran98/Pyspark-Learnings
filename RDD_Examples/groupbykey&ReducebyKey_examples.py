# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('Group by')
sc = SparkContext.getOrCreate(conf= conf)

# COMMAND ----------

file = sc.textFile('/FileStore/tables/groupby.txt')

# COMMAND ----------

flat_rdd = file.flatMap(lambda x : x.split())

# COMMAND ----------

key_rdd = flat_rdd.map(lambda x : (x,1))

# COMMAND ----------

#groupbykey_examples
key_rdd.groupByKey().mapValues(list).collect()

# COMMAND ----------

#reducebykeyexample 
key_rdd.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------

#code proficiency
sc.textFile('/FileStore/tables/groupby.txt').flatMap(lambda x : x.split()).map(lambda x:(x,1)).groupByKey().mapValues(list).collect()
