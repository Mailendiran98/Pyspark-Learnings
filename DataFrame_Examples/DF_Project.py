# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('project').getOrCreate()
from pyspark.sql.functions import min,max,col,lit,avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df = spark.read.options(InferSchema='True',Header='True').csv('/FileStore/tables/OfficeDataProject.xls')

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView('employee')

# COMMAND ----------

#1
spark.sql('select count(*) from employee').show()

# COMMAND ----------

#2
spark.sql("select count(distinct department) from employee").show()

# COMMAND ----------

spark.sql('select distinct department from employee').show()

# COMMAND ----------

spark.sql('select department,count(*) from employee group by department').show()

# COMMAND ----------

spark.sql('select state,count(*) from employee group by state').show()

# COMMAND ----------

spark.sql('select department,state,count(*) from employee group by department,state').show()

# COMMAND ----------

df.groupBy(df.department).agg(min(df.salary).alias('min'),max(df.salary).alias('max')).orderBy(col('max').asc(),col('min').asc()).show()

# COMMAND ----------

avg_bonus = df.filter(df.state=='NY').groupBy('state').agg(avg(df.bonus).alias('avg_bonus')).collect()[0]['avg_bonus']

# COMMAND ----------

df.filter((df.state=='NY')&(df.department=='Finance')&(df.bonus>avg_bonus)).show()

# COMMAND ----------

spark.sql('select (salary+500),* from employee where age > 45').show()

# COMMAND ----------

spark.sql('select * from employee where age > 45').write.csv('/FileStore/tables/employee1')

# COMMAND ----------

#9
def increment(x,y):
    if x > 45:
        return y+500
    return y

salaryDF = udf(lambda x,y :increment(x,y),IntegerType() )



df.withColumn('salary',salaryDF(df.age,df.salary)).show()
