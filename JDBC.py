#!/usr/bin/python
#-*- coding: utf8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
# PySpark
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, HiveContext
#from pyspark.sql import HiveContext, Row
from pyspark.sql.types import DataType, IntegerType
# JSON
import json
import collections
# K-Means
from numpy import array
from pyspark.mllib.clustering import KMeans
from datetime import timedelta, date


if __name__ == "__main__":
    sqlsc = SQLContext(sc)
    MYSQL_USERNAME = ""
    MYSQL_PWD = ""
    #Original URL
    MYSQL_CONNECTION_URL = "jdbc:mysql://127.0.0.1:3306/telegramdb?autoReconnect=true&useSSL=false&user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD

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
    # print all dataframe in table
    #print(dataframe_mysql_info.dtypes)
    #print(dataframe_mysql_tags.dtypes)
    #bigIpList = [item for item in bigIpList if item not in smallIpList]
    #results = info.map(lambda line: array([x[1:-1] for x in line.low]))
    #results = dataframe_mysql_info.select("low").toJSON(use_unicode=True)
    #dataframe_mysql_info.select("low").show()
    #dataframe_mysql_tags.select("low").show()
    ITtag = tags.map(lambda line: array([x.low for x in tags if x.high is 'IT']))
    results = info.map(lambda p: str(p.low))
    #parsedData = data.map(lambda line: array([int(x) for x in line.split('')]))
    for result in results.collect():
        #temp = json.loads(result, object_pairs_hook=collections.OrderedDict)
        #temp = json.dumps(result.replace('\"', ''), ensure_ascii=False, sort_keys=False, separators=(',', ':'))#.encode('utf-8')
        temp = result[1:-1]
        temp.replace('{{', '')
        temp.replace('}}', '')
        temp.replace('\"', '')

        temp = array([x for x in temp.split(",")])
        #temp = json.dumps(result, sort_keys=False, separators=(',', ':'))
        for t in temp:
            if str(t.dtype) is not str(array("").dtype):
                print("" + t + " : " + str(t.dtype))
