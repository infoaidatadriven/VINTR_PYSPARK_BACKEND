import os
import json

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col, min, max, avg, stddev, approx_count_distinct, count, to_json


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
        # spark_config.set('spark.speculation', 'false')
        # spark_config.set("spark.hadoop.hive.metastore.uris",
        #                  "thrift://hive-metastore-metastore.hms.svc.cluster.local:9083")
        # spark_config.set("spark.sql.parquet.enableVectorizedReader", "false")
        spark_config.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        spark_config.set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
        spark_config.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        spark_config.set("spark.eventlog.enabled", "true")
        spark_config.set("spark.logConf", "true")
        spark_config.set("spark.driver.cores", "6")
        spark_config.set("spark.driver.memory", "15g")
        spark_config.set("spark.executor.memory", "8g")
        spark_config.set("spark.executor.cores", "8")
        spark_config.set("spark.executor.instances", "4")

        shuffle_partitions = os.getenv("shuffle_partitions", "")
        if shuffle_partitions != "":
            spark_config.set("spark.sql.shuffle.partitions", shuffle_partitions)

        return spark_config


spark = SparkSession.builder.appName('PROFILE').getOrCreate()
# df = spark.read.csv('D:/AI-Data-Driven/Documents/Titanic01.csv', header=True, inferSchema=True)
df = spark.read.parquet('D:/AI-Data-Driven/Documents/flights_20M.parquet')

def get_count(df):
    count = {}
    num_records = df.count()
    num_columns= len(df.columns)
    grouped_df = df.groupBy(df.columns).count().filter(col("count") > 1)
    num_duplicates = grouped_df.count()

    count = {
        'nr_records':num_records,
        'nr_columns':num_columns,
        'nr_duplicates':num_duplicates
    }
    print(count)
    return count

count = get_count(df)

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
    print(result)
    return result

minimum_maximun_mean_average = min_max_mean_avg(df)

# def frequencyAnalysis(df): # This code takes 3 minutes and 20 seconds
#     frequency_Analysis = {}

#     for column in df.columns:
#         frequency_analysis = df.groupBy(column).agg(F.count("*").alias("count"))
#         frequency_Analysis[column] = frequency_analysis.collect()
    
#     print(frequency_Analysis)
#     return frequency_Analysis

# freq = frequencyAnalysis(df)

def frequencyAnalysis(df): # This code takes 1 minute 48 seconds
    frequency_Analysis = {}

    for column in df.columns:
        frequency_analysis = df.groupBy(column).agg(F.count("*").alias("count")).collect()
        frequency_Analysis[column] = frequency_analysis

    return frequency_Analysis

freq = frequencyAnalysis(df)
for column, analysis in freq.items():
    print(f"Column: {column}")
    for row in analysis:
        print(f"{row[column]}: {row['count']}")
    print()

def lengthStatics(df):
    
    column_stats = {}
    for column in df.columns:
        column_stats[column] = df.select(
            min(length(column)).alias('min_length'),
            max(length(column)).alias('max_length'),
            avg(length(column)).alias('avg_length')
        ).first()     
        
        # print(f"Column: {column}")
        # print(f"Minimum length: {column_stats[column]['min_length']}")
        # print(f"Maximum length: {column_stats[column]['max_length']}")
        # print(f"Average length: {column_stats[column]['avg_length']}")
        # print()

    print(column_stats)
    return column_stats    

length_Statics = lengthStatics(df)

def stdValue(df):
    
    std_dev={}
    for column in df.columns:
        std_dev[column] = df.select(stddev(column)).first()[0]

    print(std_dev)
    return std_dev

std_dev = stdValue(df) 

def unique_value(df):
    
    unique={}
    for column in df.columns:
        unique[column] = df.select(approx_count_distinct(column)).first()[0]

    print(unique)
    return unique

uniqueValueCount = unique_value(df) 


  









         
 

 

    

    




   