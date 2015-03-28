from pyspark import SparkContext


sc = SparkContext()
data = sc.textFile("/data/customer_sample_no_header")

eflag = data.map(lambda line: line.split('|')[8] )
eflagCounts = eflag.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
eflagCounts_list = eflagCounts.collect()
print eflagCounts_list




def productsSales(line):
	value = [x for x in line.split('|')]
	return (value[19], float(value[21]))


dataSales = data.map(lambda line: productsSales(line))
dataTotalSales_sum = dataSales.reduceByKey(lambda a, b: a + b) 
dataTotalSales_sum_list = dataTotalSales_sum.collect()

print dataTotalSales_sum_list

 


def productsSales_online(line):
	value = [x for x in line.split('|')]
	if value[8] == '1':
		value[21] = '0'

	return (value[19], float(value[21]))


dataSales_online = data.map(lambda line: productsSales_online(line))
dataTotalSales_sum_online = dataSales_online.reduceByKey(lambda a, b: a + b) 
dataTotalSales_sum_online_list = dataTotalSales_sum_online.collect() 

print dataTotalSales_sum_list
print dataTotalSales_sum_online_list



 
#print dataDistance_sum_list[0][1] / eflagCounts_list[0][1]
#print dataDistance_sum_list[1][1] / eflagCounts_list[1][1]



sc.stop()