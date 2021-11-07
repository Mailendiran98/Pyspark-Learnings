# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('min&max') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

movie_list =sc.textFile('/FileStore/tables/movie_rating.csv')
movie_list.collect()

# COMMAND ----------

movie_order = movie_list.map(lambda x : (x.split(',')[0], int(x.split(',')[1] )))

# COMMAND ----------

#provides less rating for each movie
less_rating = movie_order.reduceByKey(lambda x,y :x if x < y else y)

# COMMAND ----------

less_rating.collect()

# COMMAND ----------

#provides high rating for each movie
high_rating = movie_order.reduceByKey(lambda x,y :  x if x>y else y)

# COMMAND ----------

high_rating.collect()
