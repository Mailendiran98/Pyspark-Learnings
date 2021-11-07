# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('quiz') 
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text = sc.textFile('/FileStore/tables/groupby.txt')

# COMMAND ----------

rdd = text.flatMap(lambda x : x.split())

# COMMAND ----------

#getNumberof Partitions in rdd
rdd.getNumPartitions()

# COMMAND ----------

#repartition to given value
rdd2 = rdd.repartition(5)

# COMMAND ----------

#gives the number of partitions assigned to rdd2
rdd2.getNumPartitions()

# COMMAND ----------

rdd2.saveAsTextFile('/FileStore/tables/Output/5partitions')

# COMMAND ----------

#coalesce decreate the numberof partitions by given value
rdd3 = rdd2.coalesce(3)

# COMMAND ----------

rdd3.getNumPartitions()

# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/Output/3partitions')

# COMMAND ----------

#to see no of partitions in particular file
rdd5 =sc.textFile('/FileStore/tables/Output/3partitions')
rdd5.getNumPartitions()
