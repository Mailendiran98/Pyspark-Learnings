# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('Quiz')
sc=SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

#question-1
rdd =sc.textFile('/FileStore/tables/StudentData.csv')
rdd.count()

# COMMAND ----------

#removing header
header = rdd.first()
rdd = rdd.filter(lambda x : x!=header)

# COMMAND ----------

#question2
male_female_marks = rdd.map(lambda x :x.split(',')).map(lambda x : (x[1],x[5]))
male_female_marks.reduceByKey(lambda x,y : int(x)+int(y)).collect()

# COMMAND ----------

#question3 solution1
marks = rdd.map(lambda x : ('pass',1) if int(x.split(',')[5]) > 50 else ('Fail',1)  )
marks.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------

#question3 solution2
pass_count = rdd.filter(lambda x: int(x.split(',')[5])>50).count()
fail_count = rdd.filter(lambda x: int(x.split(',')[5])<=50).count()
print("pass count = ",pass_count)
print("Fail count = ",fail_count)

# COMMAND ----------

#question4
students_per_course = rdd.map(lambda x : (x.split(',')[3],1)).reduceByKey(lambda x,y : x+y)
students_per_course.collect()

# COMMAND ----------

#question5
marks_per_course = rdd.map(lambda x : (x.split(',')[3],int(x.split(',')[5])     )).reduceByKey(lambda x,y:x+y)
marks_per_course.collect()

# COMMAND ----------

#question6
avg_mark_percourse = rdd.map(lambda x : (x.split(',')[3],  (int(x.split(',')[5]),1)))
avg_mark_percourse = avg_mark_percourse.reduceByKey(lambda x,y :  (x[0]+y[0],x[1]+y[1]))
avg_mark_percourse = avg_mark_percourse.map(lambda x : (x[0],x[1][0]/x[1][1]))
avg_mark_percourse.collect()

# COMMAND ----------

#question6 solution2
avg_mark_percourse = rdd.map(lambda x : (x.split(',')[3],  (int(x.split(',')[5]),1)))
avg_mark_percourse = avg_mark_percourse.reduceByKey(lambda x,y :  (x[0]+y[0],x[1]+y[1]))
avg_mark_percourse.mapValues(lambda x : (x[0]/x[1])).collect()

# COMMAND ----------

#question7
marks_per_course = rdd.map(lambda x : (x.split(',')[3],int(x.split(',')[5])     ))
print('The minimum marks is',marks_per_course.reduceByKey(lambda x, y : x if x<y else y).collect())
print('The maximum marks is',marks_per_course.reduceByKey(lambda x, y : x if x>y else y).collect())

# COMMAND ----------

#question8
avg_age = rdd.map(lambda x:   (x.split(',')[1] , (int(x.split(',')[0]),1))).reduceByKey(lambda x,y :  (x[0]+y[0],x[1]+y[1]))
avg_age = avg_age.mapValues(lambda x : x[0]/x[1])
avg_age.collect()
