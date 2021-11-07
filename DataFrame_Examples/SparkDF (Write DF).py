# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min,max,col,lit
spark = SparkSession.builder.appName('WriteDF').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#df = df.groupBy('age','course').count().show()

# COMMAND ----------

df.write.options(header='True').csv('/FileStore/tables/StudentData/')

# COMMAND ----------

spark.read.options(header='True').csv('/FileStore/tables/StudentData/').show()
