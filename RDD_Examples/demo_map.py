# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('Read')
sc = SparkContext.getOrCreate(conf = conf)

# COMMAND ----------

text = sc.textFile('/FileStore/tables/dataset1.txt')

# COMMAND ----------

text.collect()

# COMMAND ----------

rdd2 = text.map(lambda a : a.split())
rdd2.collect()

# COMMAND ----------

def conversion(x):
    l1 = x.split()
    l2= []
    for i in l1:
       l2.append(int(i)) 
    
    return l2

rdd3  = text.map(conversion)
rdd3.collect()
