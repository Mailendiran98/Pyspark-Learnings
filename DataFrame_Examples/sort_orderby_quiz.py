# Databricks notebook source
from pyspark.sql import SparkSession
saprk= SparkSession.builder.appName('quiz').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/OfficeData.csv')

# COMMAND ----------

#1
df.sort('bonus').show()

# COMMAND ----------

#2
df.orderBy(df.age.desc(),df.salary.asc()).show()

# COMMAND ----------

#3
df.sort(df.age.desc(),df.bonus.desc(),df.salary.asc()).show()
