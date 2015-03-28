from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from numpy import array

# Load and parse the data
def parsePoint(line):
    values = line.split('|')
    return LabeledPoint(values[8], [values[5]] + [values[9]])
    
sc = SparkContext()
data = sc.textFile("/data/customer_sample_no_header")
parsedData = data.map(parsePoint)

# Build the model
model = LogisticRegressionWithSGD.train(parsedData)

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))
print model.weights
print parsedData.take(10)