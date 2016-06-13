from __future__ import print_function
# PySpark
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, HiveContext
#from pyspark.sql import HiveContext, Row
from pyspark.sql.types import DataType, IntegerType
# mllib for clustering
from pyspark.mllib.linalg import Vectors, DenseMatrix
from pyspark.mllib.clustering import GaussianMixture
# JSON
import json
import collections
# numpy
from numpy.testing import assert_equal
import numpy as np
from shutil import rmtree
from numpy import array
from datetime import timedelta, date

if __name__ == "__main__":
    sqlsc = SQLContext(sc)
    MYSQL_USERNAME = ""
    MYSQL_PWD = ""
    MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/telegramdb?autoReconnect=true&useSSL=false" + \
                           "&user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD
    info = sqlsc.read.format("jdbc").options(
        url = MYSQL_CONNECTION_URL,
        dbtable = "information",
        driver = "com.mysql.jdbc.Driver"
    ).load()
    tags = sqlsc.read.format("jdbc").options(
        url = MYSQL_CONNECTION_URL,
        dbtable = "tags",
        driver = "com.mysql.jdbc.Driver"
    ).load()
    # columns
    tags = tag_df.filter(tag_df.high == 'IT').map(lambda line: line.low).collect()
    cols = {}
    col_num = tag_df.filter(tag_df.high == 'IT').count()
    for tag in tags:
        cols[tag]=0

    # rows
    repos = info_df.filter(info_df.high == 'IT').map(lambda line: {line.pk_aid:json.loads(line.low,encoding="utf-8")}).collect
    rows = info_df.filter(info_df.high == 'IT').map(lambda line: {line.pk_aid:np.zeros(col_num, dtype=np.int)}).collect()
    row_num = info_df.filter(info_df.high == 'IT').count()

    for index, repo in enumerate(repos):
        for pk_aids in repo:
            elements = repo.get(pk_aids)
            for element in elements:
                for col_index, col in enumerate(cols):
                    if element.get(col) is not None:
                        rows[index].get(pk_aids)[col_index]=element.get(col)
                        print(element.get(col))
    for index, row in enumerate(rows):
        for pk_aids in row:
            if rows[index].get(pk_aids) is not None:
                if index == 0:
                    data = rows[index].get(pk_aids)
                else:
                    data = np.concatenate((data, rows[index].get(pk_aids)), axis=0)
    print(data)
    #Parameters:
    #data – RDD of data points
    #k – Number of components
    #convergenceTol – Threshold value to check the convergence criteria. Defaults to 1e-3
    #maxIterations – Number of iterations. Default to 100
    #seed – Random Seed
    #initialModel – GaussianMixtureModel for initializing learning
    model = GaussianMixture.train(data, 10, convergenceTol=0.0001,maxIterations=50)

    labels = model.predict(data).collect()

    print
