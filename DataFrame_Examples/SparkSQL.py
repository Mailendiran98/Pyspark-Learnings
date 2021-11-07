# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min,max,col,lit
spark = SparkSession.builder.appName('cache').getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True',header='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#df can create view from df and use sql queries
df.createOrReplaceTempView('Student')

# COMMAND ----------

#can use sqp inside spark by using below logic
spark.sql('select course,count(*) from Student group by course').show()
