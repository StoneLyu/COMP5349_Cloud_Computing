#!/bin/bash

spark-submit \
	--master local[16] \
	--deploy-mode client \
	--executor-cores 2\
	--num-executors 8\
	knn.py \
	--k $1 \
	--d $2