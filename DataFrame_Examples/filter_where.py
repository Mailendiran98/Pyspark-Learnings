# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('columns').getOrCreate()
from pyspark.sql.functions import col,lit

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#applying filter condition for 1 column
df2 = df.filter((col('marks')<50))
df2.show()

# COMMAND ----------

#filtering more than 1 columns
df3 = df.filter((df.marks>50) & (df.course == 'DB') & (df.gender == 'Male') & (df.roll == 111449))
df3.show()

# COMMAND ----------

#checking multiple column level condition
courses=['DB','Cloud','DSA']
df4 = df.filter((df.course.isin(courses))&(df.age == 28))
df4.show()

# COMMAND ----------

#
df5 = df.filter(df.name.endswith('ca'))
df5.show()

# COMMAND ----------

#contains will display all the names having given string in any place
df6 = df.filter(df.name.contains('la'))
df6.show()

# COMMAND ----------

#like isused same as sql
df7=df.filter(df.name.like('%Van%'))
df7.show()
