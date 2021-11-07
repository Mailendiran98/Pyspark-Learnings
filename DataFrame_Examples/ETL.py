# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,explode,count
import pyspark.sql.functions as f

# COMMAND ----------

#Extract
spark= SparkSession.builder.appName('ETL').getOrCreate()

# COMMAND ----------

df = spark.read.text('/FileStore/tables/WordData.txt')

# COMMAND ----------

#Transform
df1 = df.withColumn('splitted',f.split('value',' '))
df2 = df1.withColumn('splitted',explode('splitted'))
df3 = df2.groupBy('splitted').count()

# COMMAND ----------

#Load
df3.write.csv('/FileStore/Load')
