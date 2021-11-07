# Databricks notebook source
from pyspark import SparkConf , SparkContext

conf = SparkConf().setAppName('filter') 
sc = SparkContext.getOrCreate(conf=conf)


# COMMAND ----------

quiz = sc.textFile('/FileStore/tables/dataset1.txt')
quiz.collect()

# COMMAND ----------

#using lambda
rdd1 = quiz.filter(lambda x : x!='1 2 3 4 455')
rdd1.collect()

# COMMAND ----------

#using udf
def fun_filter(x):
    if x != '1 2 3 4 455':
        return True


rdd2 = quiz.filter(fun_filter)
rdd2.collect()

# COMMAND ----------

#satisfy all conditions
def fun_filter1(x):
    return True

rdd2 = quiz.filter(fun_filter1)
rdd2.collect()
