# Databricks notebook source
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName('streaming')
sc = SparkContext.getOrCreate(conf = conf)

ssc = StreamingContext(sc ,10)

# COMMAND ----------

rdd = ssc.textFileStream('/FileStore/tables/employee1/')

# COMMAND ----------

rdd = rdd.map(lambda x : (x,1))
rdd= rdd.reduceByKey(lambda x,y : x+y)

rdd.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(50)

# COMMAND ----------

#removes all the files present inside the directory
dbutils.fs.rm('/FileStore/tables/',True)

# COMMAND ----------

#using DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkSession Streaming').getOrCreate()
df = spark.readStream.text('/FileStore/tables')
#df.writeStream.format('console').start()

# COMMAND ----------

#doing transfromation on df before streaming it
df = df.groupBy('value').count()

# COMMAND ----------

display(df)
