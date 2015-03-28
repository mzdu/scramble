from pyspark import SparkContext


sc = SparkContext()
data = sc.textFile("/data/customer_sample_no_header")

instoreCat = data.map(lambda line: line.split('|')[8] )
instoreCatCounts = instoreCat.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
instoreCatOutput = instoreCatCounts.collect()
print instoreCatOutput




def tupleDistance(line):
	value = [x for x in line.split('|')]
	return (value[8], float(value[5]))

dataDistance = data.map(lambda line: tupleDistance(line))
dataDistance_sum = dataDistance.reduceByKey(lambda a, b: a + b)
dataDistance_sum_list = dataDistance_sum.collect()  
print dataDistance_sum_list[0][1] / instoreCatOutput[0][1]
print dataDistance_sum_list[1][1] / instoreCatOutput[1][1]
sc.stop()