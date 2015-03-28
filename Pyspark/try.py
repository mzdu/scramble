import sys
from pyspark import SparkContext

def printOutput(output):
    for (word, count) in output:
        print "%s, %i" % (word, count)

sc = SparkContext()
dataRDD = sc.textFile("/data/customer_sample_no_header")

# inflags = dataRDD.map(lambda line: line.split('|')[8])
# inflagCounts = inflags.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# inflagOutput = inflagCounts.collect()
# printOutput(inflagOutput)

# categories = dataRDD.map(lambda line: line.split('|')[19])
# categoriesCounts = categories.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# categoriesOutput = categoriesCounts.collect()
# printOutput(categoriesOutput)

instoreCat = dataRDD.map(lambda line: line.split('|')[19] if (line.split('|')[8] == '0') else None)
instoreCatCounts = instoreCat.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
instoreCatOutput = instoreCatCounts.collect()
printOutput(instoreCatOutput)

outstoreCat = dataRDD.map(lambda line: line.split('|')[19] if (line.split('|')[8] == '1') else None)
outstoreCatCounts = outstoreCat.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
outstoreCatOutput = outstoreCatCounts.collect()
printOutput(outstoreCatOutput)










sc.stop()