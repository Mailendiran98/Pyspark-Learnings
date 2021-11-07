# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('Quiz')
sc=SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[1], float(x.split(',')[2] ) ))

# COMMAND ----------

#min rating for each city
rdd3 = rdd2.reduceByKey(lambda x,y : x if x<y else y )
rdd3.collect()

# COMMAND ----------

#max rating for each city
rdd4 = rdd2.reduceByKey(lambda x,y : x if x>y else y )
rdd4.collect()
