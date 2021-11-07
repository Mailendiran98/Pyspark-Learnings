from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName('Quiz')
sc=SparkContext.getOrCreate(conf=conf)

rdd =sc.textFile('StudentData.csv')
#no-of students in file
print('--------------------------------')
print('The number students present is - ' ,rdd.count())
print('--------------------------------')