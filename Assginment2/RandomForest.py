# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import numpy as np
from numpy import *
import operator
import csv
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import lit


spark = SparkSession \
    .builder \
    .appName("Python Spark Machine Learning") \
    .getOrCreate()

train_datafile = "file:///home/stone/assign/cloudA2/Train-label-28x28.csv"
test_datafile = "file:///home/stone/assign/cloudA2/Test-label-28x28.csv"

train_df = spark.read.csv(train_datafile,inferSchema="true")
test_df = spark.read.csv(test_datafile,inferSchema="true")

train_assembler = VectorAssembler(inputCols=train_df.columns[1:],
    outputCol="features")
train_vectors = train_assembler.transform(train_df).select(col(train_df.columns[0]).alias("labels"),"features")

test_assembler = VectorAssembler(inputCols=test_df.columns[1:],
    outputCol="features")
test_vectors = train_assembler.transform(train_df).select(col(train_df.columns[0]).alias("labels"),"features")

train_featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10).fit(train_vectors)
train_labelIndexer = StringIndexer(inputCol="labels", outputCol="indexedLabel").fit(train_vectors)
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=50)
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=train_labelIndexer.labels)
pipeline = Pipeline(stages=[train_labelIndexer, train_featureIndexer, rf, labelConverter])
model = pipeline.fit(train_vectors)
predictions = model.transform(test_vectors)
predictions.select("predictedLabel", "labels", "features").show(4)
evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
print("accuracy = %g" % ( accuracy))
rfModel = model.stages[2]
print(rfModel)
spark.stop()