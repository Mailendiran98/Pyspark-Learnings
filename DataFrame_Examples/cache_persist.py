# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min,max,col,lit
spark = SparkSession.builder.appName('cache').getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True',header='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

df1 = df.withColumn('dummy',col('age')*10)

# COMMAND ----------

df2= df1.groupBy('age','gender','course').count()

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.cache()

# COMMAND ----------

df2.show()
