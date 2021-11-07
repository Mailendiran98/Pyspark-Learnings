# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('columns').getOrCreate()
from pyspark.sql.functions import col,lit

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#1
df = df.withColumn('total marks',lit(120))
df.columns

# COMMAND ----------

#2
df = df.withColumn('average',(col('marks')/col('total marks'))*100)
df.show()

# COMMAND ----------

#3
oop_df = df.filter((df.course=='OOP')&(df.average > 80))

# COMMAND ----------

#3
cloud_df = df.filter((df.course=='Cloud')&(df.average > 60))

# COMMAND ----------

oop_df.select(oop_df.name,oop_df.marks).show()
cloud_df.select(cloud_df.name,cloud_df.marks).show()
