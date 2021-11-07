# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min,max,col,lit
spark = SparkSession.builder.appName('cache').getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True',header='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

type(df)

# COMMAND ----------

rdd=df.rdd

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.filter(lambda x : x[0]==28).collect()

# COMMAND ----------

rdd.filter(lambda x : x['course']=='Cloud').collect()
