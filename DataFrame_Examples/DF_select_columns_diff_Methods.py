# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('columns').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#selecting columns using df.columns
df.select(df.columns[0:7]).show()


# COMMAND ----------

#using *
df.select('*').show()

# COMMAND ----------

#select using df
df.select(df.age,df.marks,df.name,df.course).show()

# COMMAND ----------

#select using col function
from pyspark.sql.functions import col
df.select(col('age'),col('name'),col('course')).show()

# COMMAND ----------

#filtering in other dataframe
df2 = df.select(df.columns[0:4])
df2.show()
