# Importing flask and base functions

import os
import json
import numpy as np
from datetime import datetime
from flask import Flask, Blueprint, request, render_template, send_from_directory, jsonify
from dataLineage import EntryDataLineage, dataLineageforcolumn
from dataQuality import checkQuality, checkDataQuality, savequlaitychecktodb

# Importing pyspark functions

from pyspark import SparkConf
from pyspark.shell import spark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql.functions import col, count, when, isnan, length, min, max, avg, mean, count, struct, sum, stddev_pop
from pyspark.sql.functions import stddev, to_json, collect_list, approx_count_distinct, explode, length, \
    approx_count_distinct, stddev, concat_ws
from Datacatlog import getcatalogueforcolumns
from pyspark.sql.types import FloatType

# converting this file as BluePrint to route in app.py and make it executable

preview_api = Blueprint('Preview_api', __name__)

class Connector:
        def __init__(self):
            self.spark_session = self.get_spark_session()

        @staticmethod
        def _get_spark_config():
            spark_config = SparkConf()
            spark_config.set('fs.s3a.acl.default', 'BucketOwnerFullControl')
            spark_config.set('fs.s3a.canned.acl', 'BucketOwnerFullControl')
            spark_config.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
            spark_config.set('hive.exec.dynamic.partition', 'true')
            spark_config.set('hive.exec.dynamic.partition.mode', 'nonstrict')
            spark_config.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
            spark_config.set('spark.io.compression.codec', 'snappy')
            spark_config.set('spark.shuffle.compress', 'true')
            spark_config.set('spark.scheduler.mode', 'FAIR')
            spark_config.set('spark.speculation', 'false')
            spark_config.set("spark.hadoop.hive.metastore.uris",
                             "thrift://hive-metastore-metastore.hms.svc.cluster.local:9083")
            spark_config.set("spark.sql.parquet.enableVectorizedReader", "false")
            spark_config.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            spark_config.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
            spark_config.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
            spark_config.set("spark.eventlog.enabled", "true")
            spark_config.set("spark.logConf", "true")           

            shuffle_partitions = os.getenv("shuffle_partitions", "")
            if shuffle_partitions != "":
                spark_config.set("spark.sql.shuffle.partitions", shuffle_partitions)

            return spark_config
        
@preview_api.route("/api/preview", methods=['GET'])
def preview():    
    content = request.get_json()
    sourcepath = content['sourcepath']
    
    if sourcepath is None :
        sourcepath = 'Titanic01.csv'   

    print('API Start :' , datetime.now())

    startTime = datetime.now()
    extension = sourcepath.split('.')[1]      
    print(sourcepath)
    if extension == 'csv':
        df = spark.read.csv(sourcepath, header=True, inferSchema=True)
    else :
        df = spark.read.parquet(sourcepath, header=True, inferSchema=True)

    print('API Read Completed:' , datetime.now())

    # profileDetail = get_count(df)
    # profileDetail = {}
    # profileDetail['profile'] = getColumnDetail(df)

    previewDetail = preview_endpoint(df)   
    jsonString = json.dumps(previewDetail, default=str)

    print('API End :' , datetime.now(), datetime.now() - startTime)
    return jsonString      

def get_preview(df):
    # Assuming you have a DataFrame object named `df`
    # df = spark.read.csv('D:/AI-Data-Driven/Documents/csv/Titanic01.csv', header=True)  # Replace with your DataFrame loading code

    df_preview = df.limit(1000)
    columns = df_preview.columns
    preview_data = df_preview.collect()

    preview_rows = []
    for row in preview_data:
        preview_rows.append(row.asDict())

    preview_json = {'columns': columns, 'data': preview_rows}

    return jsonify(preview_json)  

def preview_endpoint(df):
    spark = SparkSession.builder.getOrCreate()

    preview = get_preview(df)
    return preview