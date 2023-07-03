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
from pyspark.sql.functions import col, count, when, isnan, length, min, max, avg, mean, count, struct, sum
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

@profile_api.route("/api/profile_pyspark", methods=['POST'])
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

def get_count(df):
    count = {}
    num_records = df.count()
    num_columns = len(df.columns)
    grouped_df = df.groupBy(df.columns).count().filter(col("count") > 1)
    num_duplicates = grouped_df.count()
    null_counts = df.select([
        sum(when(col(column).isNull() | (col(column) == ""), 1).otherwise(0)).alias(column)
        for column in df.columns
    ])
    total_null_count = reduce(lambda x, y: x + y, null_counts.first().asDict().values())

    count = {
        'nr_records': num_records,
        'nr_columns': num_columns,
        'nr_duplicates': num_duplicates,
        'null_counts': total_null_count
    }

    return count

def get_null_values_count(df):
    null_counts = df.select([
        sum(when(col(column).isNull() | (col(column) == ""), 1).otherwise(0)).alias(column)
        for column in df.columns
    ])

    null_values_count_dict = null_counts.first().asDict()

    return null_values_count_dict


def min_max_mean_avg(df):

    result = {}
    min_suffix = "min_"
    max_suffix = "max_"
    mean_suffix = "mean_"
    avg_suffix = "avg_"

    for column in df.columns:
        result[column] = df.\
        select( F.min(column).alias(f"Minimum:{min_suffix}{column}"),
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

def staticalAnalysis(df):
    get_count(df)
    min_max_mean_avg(df)
    lengthStatics(df)
    unique_value(df)
    dataType(df)

    combined= {
        'count': get_count(df),
        'unique': unique_value(df),
        'std': stdValue(df),
        'minmaxmeanavg': min_max_mean_avg(df),
        'lengthstatics': lengthStatics(df),
        'dataType': dataType(df)
        }

    return combined



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

    data_types = {}

    
    for column in df.columns:
        data_type = str(df.schema[column].dataType)
        
        if data_type == "IntegerType()" or data_type == "DoubleType()":
            data_types[column] = "Numeric"

        elif data_type == "datetime64[ns]" or data_type == "datetime":
            data_types[column] = "Alphabetic"

        elif data_type == "StringType()":
            data_types[column] = "Alphanumeric"

    return data_types

def frequencyAnalysis(df):

    frequency_Analysis = {}
    for column in df.columns:
        frequency_analysis = df.groupBy(column).agg(F.count("*").alias("count")).collect()
        frequency_Analysis[column] = [(row[0], row[1]) for row in frequency_analysis]

    # print(frequency_Analysis)
    return frequency_Analysis

def maskAnalysis(df):
    alphabetic_cols = [col_name for col_name, data_type in df.dtypes if
                       data_type == "string" and df.select(col(col_name).rlike("[a-zA-Z]")).first()[0]]

    mask = {}

    for column in alphabetic_cols:
        counts = df.groupBy(column).agg(count("*").alias("count"))
        counts_data = counts.collect()
        counts_dict = {}
        total_count = 0  # Initialize total count for the column
        for row in counts_data:
            column_value = row[column]
            count_value = row["count"]
            if column_value is not None:
                if isinstance(column_value, str):
                    counts_dict["L" + str(len(column_value))] = count_value
                else:
                    counts_dict["L" * int((column_value))] = count_value  # Convert column_value to integer
            else:
                counts_dict["L"] = count_value       
            total_count += count_value
        mask[column] = counts_dict

    return mask

def get_pattern(df):

    alphabetic_cols = [col_name for col_name, data_type in df.dtypes if
                       data_type == "string" and df.select(col(col_name).rlike("[a-zA-Z]")).first()[0]]
    pattern = {}

    for column in alphabetic_cols:
        counts = df.groupBy(column).agg(count("*").alias("count"))
        counts_data = counts.collect()
        counts_dict = {}
        total_count = 0
        unique_value = "W"
        for row in counts_data:
            count_value = row["count"]
            total_count += count_value
            if unique_value is None:
                unique_value = row[column]
        counts_dict[unique_value]  = total_count  # Include the total count in the unique value representation

        # counts_dict["total_count"] = total_count
        pattern[column] = counts_dict

    return pattern


# def get_correlation(df):
#     # Convert all columns to float type
#     df = df.limit(20)
#     for col_name in df.columns:
#         df = df.withColumn(col_name, col(col_name).cast("float"))

#     # Calculate the correlation value of each column with all other columns
#     correlations = {}
#     for col_name in df.columns:
#         if col_name != "id":  # Skip the "id" column if present
#             corr_values = {}
#             for other_col in df.columns:
#                 if other_col != col_name:
#                     corr_val = df.stat.corr(col_name, other_col)
#                     corr_values[other_col] = corr_val
#             correlations[col_name] = corr_values

#     return correlations

# def find_outliers_in_df(df):
#     outliers_dict = {}
#     label_column = df.columns[-1]
#     for column_name in df.columns[:-1]:
#         column_stats = df.select(mean(col(column_name)), stddev_pop(col(column_name))).first()
#         mean_value = column_stats[0]
#         stddev_value = column_stats[1]
#         if stddev_value is None:
#             continue
#         lower_bound = mean_value - (3 * stddev_value)
#         upper_bound = mean_value + (3 * stddev_value)
#         outliers = df.filter((col(column_name) < lower_bound) | (col(column_name) > upper_bound))
#         outliers_dict[column_name] = outliers   
#     return outliers_dict

def profile_endpoint(df):
    spark = SparkSession.builder.getOrCreate()

    profile = get_count(df), get_null_values_count(df), min_max_mean_avg(df), lengthStatics(df), stdValue(df), unique_value(df), dataType(df), frequencyAnalysis(df), maskAnalysis(df), get_pattern(df)
    return profile


if __name__ == '__main__' :
    profile_api.run(debug=True)











