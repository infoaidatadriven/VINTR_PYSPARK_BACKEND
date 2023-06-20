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
from pyspark.sql.functions import col, count, when, isnan, length, min, max, avg, mean, count, struct
from pyspark.sql.functions import stddev, to_json, collect_list, approx_count_distinct, explode, length, \
    approx_count_distinct, stddev, concat_ws
from Datacatlog import getcatalogueforcolumns
from pyspark.sql.types import FloatType

# converting this file as BluePrint to route in app.py and make it executable

profile_api = Blueprint('profile_api', __name__)

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

@profile_api.route("/api/profile", methods=['POST'])
def profile():    
    content = request.get_json()
    sourcepath = content['sourcepath']

    df = spark.read.csv(sourcepath, header=True, inferSchema=True)

    profileDetail = get_count(df)
    profileDetail['profile'] = getColumnDetail(df)

    jsonString = json.dumps(profileDetail, default=str)
    return jsonString

def get_count(df):
    count = {}
    num_records = df.count()
    num_columns = len(df.columns)
    grouped_df = df.groupBy(df.columns).count().filter(col("count") > 1)
    num_duplicates = grouped_df.count()

    count = {
        'nr_records': num_records,
        'nr_columns': num_columns,
        'nr_duplicates': num_duplicates
    }

    return count

def min_max_mean_avg(df):

    result = {}
    min_suffix = "min_"
    max_suffix = "max_"
    mean_suffix = "mean_"
    avg_suffix = "avg_"

    for column in df.columns:
        result[column] = df.\
        select( F.min(column).alias(f"{min_suffix}{column}"),
            F.max(column).alias(f"{max_suffix}{column}"),
            F.mean(column).alias(f"{mean_suffix}{column}"),
            F.avg(column).alias(f"{avg_suffix}{column}")).collect()[0]
   
    # print(result)
    return result

def lengthStatics(df):
    
    column_stats = {}
    for column in df.columns:
        column_stats[column] = df.select(
            min(length(column)).alias('min_length'),
            max(length(column)).alias('max_length'),
            avg(length(column)).alias('avg_length')
        ).first()    

    # print(column_stats)
    return column_stats 

def stdValue(df):    
    std_dev={}
    for column in df.columns:
        std_dev[column] = df.select(stddev(column)).first()[0]

    # print(std_dev)
    return std_dev

def unique_value(df):
    
    unique={}
    for column in df.columns:
        unique[column] = df.select(approx_count_distinct(column)).first()[0]

    # print(unique)
    return unique

def dataType(df):

    for column in df.columns:
        data_type=str(df.schema[column].dataType)    

    # print(data_type)
    return(data_type)  

def frequencyAnalysis(df):

    frequency_Analysis = {}
    for column in df.columns:
        frequency_analysis = df.groupBy(column).agg(F.count("*").alias("count")).collect()
        frequency_Analysis[column] = [(row[0], row[1]) for row in frequency_analysis]

    # print(frequency_Analysis)
    return frequency_Analysis 

def getColumnDetail(df):
    result = {}

    for column in df.columns:
        result[column] = df.select(F.min(column).alias('min_value'),
            F.max(column).alias('max_value'),
            F.mean(column).alias('mean_value'),
            F.avg(column).alias('avg_value'),
            min(length(column)).alias('min_length'),
            max(length(column)).alias('max_length'),
            avg(length(column)).alias('avg_length'),
            stddev(column).alias('std_dev'),
            approx_count_distinct(column).alias('unique')).first()
        #result[column]['dataType'] = df.schema[column].dataType
    return result
 
if __name__ == '__main__' :
    profile_api.run(debug=True)











