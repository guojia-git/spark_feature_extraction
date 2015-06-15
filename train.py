from pyspark.mllib.classification import LabeledPoint, LogisticRegressionWithSGD
from numpy import array
import math


def parsePoint(line):
  #values = [float(x) for x in line.replace('\n', ',').split(',')]
  values = [float(x) for x in line.split(',')]
  return LabeledPoint(values[len(values)-1], values[1:len(values)-1])

data = sc.textFile("new_features.csv")
parsedData = data.map(parsePoint)

model = LogisticRegressionWithSGD.train(parsedData)

valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
#MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce((lambda x, y: x + y)/ valuesAndPreds.count())
#MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce((lambda x, y: x + y))

correct_cnt = valuesAndPreds.map(lambda (v, p): 1 if float(v)==float(p) else 0).reduce((lambda x, y: x + y))
cnt = valuesAndPreds.map(lambda (v, p): 1).reduce((lambda x, y: x + y))
print("Mean Squared Error = " + str(MSE/cnt))
print("Accuracy = " + str(correct_cnt/float(cnt)))
