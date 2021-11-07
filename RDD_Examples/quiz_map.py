from pyspark import SparkConf , SparkContext

conf = SparkConf().setAppName('Quiz') 
sc = SparkContext.getOrCreate(conf=conf)

quiz = sc.textFile('mapquiz.txt')
quiz.collect()

#using function
def word_count(x):
	l1 = x.split()
	l2 = []
	for i in l1:
		l2.append(len(i))
	return l2

rdd = quiz.map(word_count)
print('This is the output : ',rdd.collect())

#using lambda function
rdd2=quiz.map(lambda x : [len(i) for i in x.split()])
print('Lambda output is ',rdd2.collect())

