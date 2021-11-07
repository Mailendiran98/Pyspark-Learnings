# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('columns').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

df2 = df.select(col('name').alias('Full Name'),col('roll').alias('Roll number'))
df2.show()
