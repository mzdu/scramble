from pyspark import SparkContext


sc = SparkContext()
data = sc.textFile("/data/customer_sample_no_header")


eflag = data.map(lambda line: line.split('|')[8] )
eflagCounts = eflag.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
eflagCounts_list = eflagCounts.collect()
print eflagCounts_list




def tupleDistance(line):
	value = [x for x in line.split('|')]
	return (value[8], float(value[5]))

dataDistance = data.map(lambda line: tupleDistance(line))
dataDistance_sum = dataDistance.reduceByKey(lambda a, b: a + b)
dataDistance_sum_list = dataDistance_sum.collect()  
print dataDistance_sum_list[0][1] / eflagCounts_list[0][1]
print dataDistance_sum_list[1][1] / eflagCounts_list[1][1]



def tupleDiscount(line):
	value = [x for x in line.split('|')]
	return (value[8], float(value[22]))

dataDiscount = data.map(lambda line: tupleDiscount(line))
dataDiscount_sum = dataDiscount.reduceByKey(lambda a, b: a + b)
dataDiscount_sum_list = dataDiscount_sum.collect()   
print dataDiscount_sum_list[0][1] / eflagCounts_list[0][1]
print dataDiscount_sum_list[1][1] / eflagCounts_list[1][1]

def tupleSales(line):
	value = [x for x in line.split('|')]
	return (value[8], float(value[21]))

dataSales = data.map(lambda line: tupleSales(line))
dataSales_sum = dataSales.reduceByKey(lambda a, b: a + b)
dataSales_sum_list = dataSales_sum.collect()  

print 'distance:'
print dataDistance_sum_list[0][1] / eflagCounts_list[0][1]
print dataDistance_sum_list[1][1] / eflagCounts_list[1][1]

print 'discount:'
print dataDiscount_sum_list[0][1] / eflagCounts_list[0][1]
print dataDiscount_sum_list[1][1] / eflagCounts_list[1][1]

print 'sales:'
print dataSales_sum_list[0][1] / eflagCounts_list[0][1]
print dataSales_sum_list[1][1] / eflagCounts_list[1][1]

  




sc.stop()