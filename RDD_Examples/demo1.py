# Databricks notebook source
from pyspark import SparkConf,SparkContext

# COMMAND ----------

conf = SparkConf().setAppName('Reading')

# COMMAND ----------

sc = SparkContext.getOrCreate(conf = conf)


# COMMAND ----------

text = sc.textFile('dataset1.txt')      

# COMMAND ----------


print('The textfile has below output ',text.collect())

