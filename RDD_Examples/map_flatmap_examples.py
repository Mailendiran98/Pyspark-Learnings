# Databricks notebook source
from pyspark import SparkConf , SparkContext

conf = SparkConf().setAppName('map_flatmap_examples') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

quiz = sc.textFile('/FileStore/tables/mapquiz.txt')
quiz.collect()

# COMMAND ----------

#using functions
def word_count(x):
	l1 = x.split()
	l2 = []
	for i in l1:
		l2.append(len(i))
	return l2

# COMMAND ----------

rdd = quiz.map(word_count)
print('This is the output : ',rdd.collect())

# COMMAND ----------

#using lambda functions
rdd2=quiz.map(lambda x : [len(i) for i in x.split()])
print('Lambda output is ',rdd2.collect())

# COMMAND ----------

#flatmap
rdd3 = quiz.flatMap(lambda x:x.split())
rdd3.collect()

# COMMAND ----------


text2 = sc.textFile('/FileStore/tables/dataset1.txt')
text2.collect()

# COMMAND ----------

#lambda function
rdd4 = text2.flatMap(lambda x : x.split())
rdd4.collect()

# COMMAND ----------

#using UDF
def fun_flatmp(x):
    return x

rdd5 = text2.flatMap(fun_flatmp)
rdd5.collect()
