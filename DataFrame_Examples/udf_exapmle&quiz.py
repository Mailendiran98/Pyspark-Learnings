# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
from pyspark.sql.types import IntegerType,FloatType
spark = SparkSession.builder.appName('UDF').getOrCreate()
df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')

# COMMAND ----------

def get_salary(x,y):
    return x+y

salaryDF = udf(lambda x,y:get_salary(x,y),IntegerType())


salaryDF = df.withColumn('Updated Salary',salaryDF(df.salary,df.bonus))

# COMMAND ----------

salaryDF.show()

# COMMAND ----------

#calculate increment for employees based on state

def get_increment(x,y,z):
    if x=='NY':
       increment = ((y/100)*10 + (z/100)*5)
    elif x=='CA':
       increment = ((y/100)*12 + (z/100)*3)
    
    return increment;

IncrementDF = udf(lambda x,y,z :get_increment(x,y,z),FloatType())



increment_df = df.withColumn('Increment',IncrementDF(df.state , df.salary,df.bonus))

# COMMAND ----------

increment_df.show()
