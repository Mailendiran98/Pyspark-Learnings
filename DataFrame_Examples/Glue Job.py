# Databricks notebook source
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Glue').getOrCreate()

# COMMAND ----------

#full load
fldf = spark.read.options(inferschema = 'True').csv('/FileStore/tables/LOAD00000001.csv')
fldf = fldf.withColumnRenamed('_c0','id').withColumnRenamed('_c1','name').withColumnRenamed('_c2','city')
fldf.write.mode('overwrite').csv('/FileStore/tables/OutputFiles/outputfile.csv')

# COMMAND ----------

#realtime data
update_df = spark.read.options(inferschema = 'True').csv('/FileStore/tables/20211103_052641332.csv')
update_df = update_df.withColumnRenamed('_c0','action').withColumnRenamed('_c1','id').withColumnRenamed('_c2','name').withColumnRenamed('_c3','city')
ffdf = spark.read.csv('/FileStore/tables/OutputFiles/outputfile.csv')

# COMMAND ----------

display(ffdf)

# COMMAND ----------

for row in update_df.collect():
    if row['action'] =='D':
        ffdf = ffdf.filter(ffdf._c0 != row['id'])
    if row['action'] == 'U':
        ffdf = ffdf.withColumn('_c1',when(ffdf._c0== row['id'] , row['name']).otherwise(ffdf._c1))
        ffdf = ffdf.withColumn('_c2',when(ffdf._c0==row['id'],row['city']).otherwise(ffdf._c2))
    if row['action'] == 'I':
        value =[list(row[1:])]
        columns=['_c0','_c1','_c2']
        newdf = spark.createDataFrame(value,columns)
        ffdf = ffdf.union(newdf)

# COMMAND ----------

ffdf.write.mode('overwrite').csv('/FileStore/tables/OutputFiles/outputfile.csv')
