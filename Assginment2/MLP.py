from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import numpy as np
from numpy import *
import operator
import csv
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier

spark = SparkSession\
    .builder\
    .appName("RandomForestClassifierExample")\
    .getOrCreate()

train_datafile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Test-label-28x28.csv"
test_datafile = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/MNIST/Train-label-28x28.csv"

train_df = spark.read.csv(train_datafile,inferSchema="true")
test_df = spark.read.csv(test_datafile,inferSchema="true")

layers = [784, 100, 10]

# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=30, seed=1234)

# train
train_assembler = VectorAssembler(inputCols=train_df.columns[1:], outputCol="features")
train_vectors = train_assembler.transform(train_df).select(col(train_df.columns[0]).alias("label"),"features")

test_assembler = VectorAssembler(inputCols = test_df.columns[1:], outputCol="features")
test_vectors = test_assembler.transform(test_df).select(col(train_df.columns[0]).alias("label"), "features")

model = trainer.fit(train_vectors)

test_result = model.transform(test_vectors)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(test_result)
print("Test Error = %g" % (1.0 - accuracy))


print("args:",layers)
print("Accuracy:",accuracy)

spark.stop()