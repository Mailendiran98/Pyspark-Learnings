# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('columns').getOrCreate()
from pyspark.sql.functions import col,lit

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#count
df.filter(df.name.startswith('J')).count()

# COMMAND ----------

#distinct
df.select('gender','age').distinct().show()

# COMMAND ----------

#drop dupliocates will remove duplicate rows
df.dropDuplicates(['age','course']).show()
