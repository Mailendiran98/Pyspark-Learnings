# Databricks notebook source
from pyspark.sql import SparkSession
saprk= SparkSession.builder.appName('quiz').getOrCreate()
from pyspark.sql.functions import avg,sum,min,max,count,mean,col,lit

# COMMAND ----------

df = spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

#doing group by along with aggregate functions
df.groupBy('gender').avg('marks').show()

# COMMAND ----------

df.groupBy('gender').count().show()
df.groupBy('age').count().show()
df.groupBy('course').count().show()
df.groupBy('course').avg('marks').show()

# COMMAND ----------

df.groupBy('course').avg('marks').show()

# COMMAND ----------

df.groupBy('course').mean('marks').show()

# COMMAND ----------

df.groupBy('gender','course').avg('marks').sort('gender').show()

# COMMAND ----------

df.groupBy('gender','course').count().show()

# COMMAND ----------

df.groupBy('course').agg(count('*'),sum('marks'),min('marks'),max('marks'),avg('marks')).show()

# COMMAND ----------

#grouping multiple columns and using multiple aggregate functions and giving alias name
df.groupBy('course','gender').agg(count('*').alias('Total Count'),sum('marks').alias('Total Marks'),min('marks'),max('marks'),avg('marks')).sort('gender').show()

# COMMAND ----------

#group by and filter
df2 = df.filter(df.gender=='Male').groupBy('gender','course').agg(count('*').alias('count'))
df2.show()

# COMMAND ----------

#filtering/where after group by ...
df2.filter(df.course == 'Cloud').show()
df2.where(df.course == 'Cloud').show()
