# Databricks notebook source
from pyspark import SparkConf ,SparkContext
conf = SparkConf().setAppName('RDDtoDF')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
header = rdd.first()
rdd = rdd.filter(lambda x:x!=header).map(lambda x:x.split(','))
rdd = rdd.map(lambda x : [int(x[0]),x[1],x[2],x[3],x[4],int(x[5]),x[6]])
rdd.collect()

# COMMAND ----------

#creating DF from existing RDD
schema = header.split(',')
dfRDD = rdd.toDF(schema)
dfRDD.show()
dfRDD.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType
schema = StructType([
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("name",StringType(),True),
    StructField("course",StringType(),True),
    StructField("roll",StringType(),True),
    StructField("marks",IntegerType(),True),
    StructField("email",StringType(),True)   
])

# COMMAND ----------

#creating dataframe using defined schema
dfRDD = spark.createDataFrame(rdd,schema=schema)
dfRDD.printSchema()
dfRDD.show()

# COMMAND ----------

#passing defined schema to check datatypes
dfRDD = rdd.toDF(schema)
dfRDD.printSchema()
dfRDD.show()
