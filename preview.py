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

# converting this file as BluePrint to route in app.py and make it executable

profile_api = Blueprint('preview', __name__)

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
        

@profile_api.route("/api/profile_preview", methods=['POST'])
def profile():    
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

    profileDetail = profile_endpoint(df)   
    jsonString = json.dumps(profileDetail, default=str)

    print('API End :' , datetime.now(), datetime.now() - startTime)
    return jsonString   

def get_preview(df):

    preview = {
        df.show(truncate=False)
    }

    return preview

def profile_endpoint(df):
    spark = SparkSession.builder.getOrCreate()

    profile = get_preview(df)
    return profile
          
