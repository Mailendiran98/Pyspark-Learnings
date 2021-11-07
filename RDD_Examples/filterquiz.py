# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('Filter Quiz solution')
sc = SparkContext.getOrCreate(conf = conf)

# COMMAND ----------

text = sc.textFile('/FileStore/tables/filterquiz.txt')

# COMMAND ----------

rdd = text.flatMap(lambda x : x.split())

# COMMAND ----------

#function to remove values starting from a or c
def filterAandC(x):
    if x.startswith('a') or x.startswith('c'):       
        return False
    return True
    
rdd2 = rdd.filter(filterAandC)
rdd2.collect()

# COMMAND ----------

#using lambda function
rdd3 = rdd.filter(lambda x : not(x.startswith('a') or x.startswith('c')))
rdd3.collect()
