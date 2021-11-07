# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('quiz') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_rating.csv')

# COMMAND ----------

#putting in tuple & converting to int and adding 1 inside tuple for adding
rdd2 = rdd.map(lambda x : (x.split(',')[0],(int(x.split(',')[1]),1)  ))
#reducebykey adds the value
rdd3 = rdd2.reduceByKey(lambda x,y :(x[0]+y[0],x[1]+y[1]))
#doing average by / 
rdd4 = rdd3.map(lambda x : (x[0],round(x[1][0]/x[1][1] ,1)))

# COMMAND ----------

rdd4.collect()
