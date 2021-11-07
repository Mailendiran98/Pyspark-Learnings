# Databricks notebook source
from pyspark.sql import SparkSession
saprk= SparkSession.builder.appName('quiz').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#using drop duplicates
df.select('age','gender','course').dropDuplicates(['age','gender','course']).show()

# COMMAND ----------

#using distinct
df.select('age','gender','course').distinct().show()
