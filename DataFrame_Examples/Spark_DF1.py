# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark DataFrame').getOrCreate()

# COMMAND ----------

df = spark.read.option('header',True).csv('/FileStore/tables/StudentData.csv')
df.show()
