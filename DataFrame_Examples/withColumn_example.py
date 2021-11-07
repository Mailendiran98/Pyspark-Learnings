# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('columns').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#converting int to string using withColumn&col
from pyspark.sql.functions import col,lit
df2 = df.withColumn('roll',col('roll').cast('string'))
df2.printSchema()

# COMMAND ----------

#column manipulation using withColumn
df3 = df2.withColumn('marks',col('marks')+10)
df3.show()

# COMMAND ----------

#adding new column
df4 = df3.withColumn('10marksadded',col('marks')+10)
df4.show()

# COMMAND ----------

#renaming column
df5 = df4.withColumnRenamed('10marksadded','+10Marks')
df5.show()

# COMMAND ----------

#adding new column in hardcoded value
df6 = df5.withColumn('country',lit('India'))
df6.show()

# COMMAND ----------

#multiple manipulations in single DF
df7 = df6.withColumn('marks',col('marks')-10).withColumn('country',lit('USA')).withColumnRenamed('+10Marks','+20Marks')
df7.show()
