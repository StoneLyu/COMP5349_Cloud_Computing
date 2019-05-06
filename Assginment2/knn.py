# coding: utf-8

# Import all necessary libraries and setup the environment for matplotlib
# %matplotlib inline
# import findspark
# findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA
import argparse
#from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import numpy as np
#import matplotlib.pyplot as plt
#from numpy import *
import operator
import csv
from sklearn.metrics import precision_recall_fscore_support as score
import time


spark = SparkSession \
    .builder \
    .appName("Python Spark Machine Learning") \
    .getOrCreate()
sc = spark.sparkContext

starttime=time.time()

# csv    
train_datafile = "file:///home/stone/Train-label-28x28.csv"
test_datafile = "file:///home/stone/Test-label-28x28.csv"

num_test_samples = 60000

parser = argparse.ArgumentParser()
parser.add_argument("--k", default=10)
parser.add_argument("--d", default=50)
args = parser.parse_args()
pca_d = int(args.d)
k_num = int(args.k)

print("d: ",pca_d,"k: ", k_num)

# DataFrame
train_df = spark.read.csv(train_datafile,inferSchema="true")
test_df = spark.read.csv(test_datafile,inferSchema="true")

# transform train data into vector
# get train label
train_label_assembler = VectorAssembler(inputCols=train_df.columns[0:1],
    outputCol="features")
# get train data
train_assembler = VectorAssembler(inputCols=train_df.columns[1:],
    outputCol="features")

train_label_vectors = train_label_assembler.transform(train_df).select("features")
train_vectors = train_assembler.transform(train_df).select("features")

# train_label_vectors.show(5)
# train_vectors.show(5)

# transform test data into vector
# get test label
test_label_assembler = VectorAssembler(inputCols=test_df.columns[0:1],
    outputCol="features")
# get test data
test_assembler = VectorAssembler(inputCols=test_df.columns[1:],
    outputCol="features")

test_label_vectors = test_label_assembler.transform(test_df).select("features")
test_vectors = test_assembler.transform(test_df).select("features")

pca = PCA(k=pca_d, inputCol="features", outputCol="pca")
model = pca.fit(train_vectors)
pca_result = model.transform(train_vectors).select('pca')

test_pca_result = model.transform(test_vectors).select('pca')

local_pca=np.array(pca_result.collect())#train data
local_label=np.array(train_label_vectors.collect())#train label

test_local_pca=np.array(test_pca_result.collect())#test data
test_local_label=np.array(test_label_vectors.collect())#test label

local_pca=local_pca.reshape((60000,50))
local_label=local_label.reshape((60000,1))
local_label=local_label[:,0]

test_local_pca=test_local_pca.reshape((10000,50))
test_local_label=test_local_label.reshape((10000,1))
test_local_label=test_local_label[:,0]

test_data_pca_rdd = sc.parallelize(test_local_pca)

def KNNFunction(inX):
    #inX = test_local_pca
    
    global count
    global tmplabel
    classCount = {}
    dataset = local_pca
    labels = local_label
    k = k_num

    dataSetSize = dataset.shape[0]

    diffMat = tile(inX, (dataSetSize, 1)) - dataset
    sqDiffMat = diffMat ** 2
    sqDistance = sqDiffMat.sum(axis=1)
    distance = sqDistance ** 0.5
    
    sortedDistIndicies = distance.argsort()
    
    for i in range(k):
        voteIlabel = labels[sortedDistIndicies[i]]
        classCount[voteIlabel] = classCount.get(voteIlabel, 0) + 1
        
    sortedClassCount = sorted(classCount.items(), key=operator.itemgetter(1), reverse=True)
    
    return int(round(sortedClassCount[0][0]))
	
gain_label_rdd = test_data_pca_rdd.map(KNNFunction)

gain_label = gain_label_rdd.collect()

for i in range(10):
    TP = FP = TN = FN = 0
    
    for j in range(10000):
        test_label = test_local_label[j]
        g_label = gain_label[j]
        
        if i == g_label:
            if g_label == test_label:
                TP += 1
            else:
                FP += 1
        else:
            if g_label == test_label:
                TN += 1
            else:
                FN += 1
    if (TP+FP) != 0:
        P = TP / (TP + FP)
    else:
        P = 0
        
    if (TP+FN) != 0:
        R = TP/(TP+FN)
    else:
        R = 0
        
    A = (TP+TN)/10000
    
    f1_score = 2*P*R/(P+R)
    
    print('%d: precision: %.2f%%'%(i, (P*100)))
    print('%d: accuracy: %.2f%%'%(i, (A*100)))
    print('%d: recall: %.2f%%'%(i, (R*100)))
    print('%d: f1_score: %.2f%%\n'%(i, (f1_score*100)))
	
endtime=time.time()
print("time:", endtime-starttime, "s")

spark.stop()