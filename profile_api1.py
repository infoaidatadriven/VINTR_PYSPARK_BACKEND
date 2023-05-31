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


@profile_api.route("/api/profile", methods=['POST'])
def profile():
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

    content = request.get_json()
    sourcepath = content['sourcepath']
    df = spark.read.csv(sourcepath, header=True, inferSchema=True)

    # Get the number of records in the DataFrame
    num_records = df.count()

    # Get the number of columns in the DataFrame
    num_columns = len(df.columns)

    columns = df.columns
    input_dict = {"profile": []}
    input_dict['nr_totalrecords'] = num_records
    input_dict['nr_totalcols'] = num_columns

    row_string = concat_ws(",", *df.columns)
    df_with_row_string = df.withColumn("row_string", row_string)
    duplicates = df_with_row_string.groupBy("row_string").agg(count("*").alias("count")).filter(
        "count > 1").count()

    ListResult = []


    for column in columns:
        details = {}
        attributeSummary = {}
        LengthStatics = {}
        valueStatistics = {}
        frequencyAnalysis = {}
        staticalAnalysis = {}
        maskAnalysis = {}
        patternAnalysis = {}
        outliersList = {}
        outliersPercent = {}
        correlationSummary = {}
        dq = {}

        num_duplicates = duplicates
        null_records = df.filter(df[column].isNull()).count()
        data_type = str(df.schema[column].dataType)
        unique_values_count = df.select(approx_count_distinct(column)).first()[0]
        std_dev = df.select(stddev(column)).first()[0]

        lengths = df.select([length(col(c)).alias(c) for c in df.columns])
        lengths = lengths.fillna(0)
        for c in lengths.columns:
            lengths = lengths.withColumn(c, col(c).cast("int"))
        length_stats = lengths.agg(
            *[min(c).alias("Min_" + c) for c in lengths.columns],
            *[max(c).alias("Max_" + c) for c in lengths.columns],
            *[avg(c).alias("Average_" + c) for c in lengths.columns]
        )
        mean_length = df.select(mean(length(column))).first()[0]

        frequency_analysis = df.groupBy(column).agg(count("*").alias("count")).orderBy(col("count").desc())
        json_freq = frequency_analysis.toJSON().collect()

        def get_attributeSummary():
            attributeSummary['duplicates'] = num_duplicates
            attributeSummary['null_records'] = null_records
            attributeSummary['records'] = num_records
            attributeSummary['outliers'] = 0
            attributeSummary['invalid'] = 0
            return

        def get_frequencyAnalysis():
            frequencyAnalysis['frequencyAnalysis'] = json_freq
            details['frequencyAnalysis'] = frequencyAnalysis
            return

        def get_maskAnalysis():
            maskAnalysis['maskAnalysis'] = []
            details['maskAnalysis'] = maskAnalysis
            return

        def get_patternAnalysis():
            patternAnalysis['patternAnalysis'] = []
            details['patternAnalysis'] = patternAnalysis
            return

        def get_outliersList():
            outliersList['outliersList'] = []
            details['outliersList'] = outliersList
            return

        def get_outliersPercent():
            outliersPercent['outliersPercent'] = {}
            details['outliersPercent'] = outliersPercent
            return

        def get_correlationSummary():
            correlationSummary['positiveSummary'] = 0
            correlationSummary['negativeSummary'] = 0
            details['correlationSummary'] = correlationSummary
            return

        def get_dq():
            dq['dq'] = 0
            details['dq'] = dq
            return

        def get_value_lengthStatics():
            LengthStatics['Min'] = int(length_stats.first()["Min_" + column])
            LengthStatics['Max'] = int(length_stats.first()["Max_" + column])
            LengthStatics['Average'] = round(length_stats.first()["Average_" + column], 2)
            # LengthStatics['Median'] = int(median_stats[column])
            details['LengthStatics'] = LengthStatics
            return

        def get_lengthStatics():
            staticalAnalysis['count'] = num_records
            staticalAnalysis['Nullcount'] = null_records
            staticalAnalysis['UniqueValuesCount'] = unique_values_count
            staticalAnalysis['MinimumValue'] = df.select(F.min(column)).collect()[0][0]
            staticalAnalysis['MeanValue'] = df.select(F.mean(column)).collect()[0][0]
            staticalAnalysis['MedianValue'] = df.select(F.avg(column)).collect()[0][0]
            staticalAnalysis['MaximumValue'] = df.select(F.max(column)).collect()[0][0]
            staticalAnalysis['Std_Dev'] = std_dev
            staticalAnalysis['minLength'] = int(length_stats.first()["Min_" + column])
            staticalAnalysis['maxLength'] = int(length_stats.first()["Max_" + column])
            staticalAnalysis['MeanLength'] = mean_length
            # staticalAnalysis['MedianLength'] = int(median_stats[column])
            staticalAnalysis['Data Type'] = data_type
            staticalAnalysis['suggested_dtype'] = ""
            details['staticAnalysis'] = staticalAnalysis
            return

        if (data_type == "IntegerType()" or data_type == "DoubleType()"):
            details['column'] = column
            attributeSummary['dataType'] = "Numeric"
            def get_valueStatics():
                valueStatistics["MinimumValue"] = df.select(F.min(column)).collect()[0][0]
                valueStatistics["MinValLength"] = int(length_stats.first()["Min_" + column])
                valueStatistics["MaximumValue"] = df.select(F.max(column)).collect()[0][0]
                valueStatistics["MaxValLength"] = int(length_stats.first()["Max_" + column])
                valueStatistics["MeanValue"] = df.select(F.mean(column)).collect()[0][0]
                valueStatistics["MedianValue"] = df.select(F.avg(column)).collect()[0][0]
                valueStatistics["UniqueValuesCount"] = unique_values_count
                valueStatistics["Std_Dev"] = std_dev
                details['valueStatistics'] = valueStatistics
                return

        if (data_type == "datetime64[ns]" or data_type == "datetime"):
            details['column'] = column
            attributeSummary['dataType'] = "Alphabetic"

        if (data_type == "StringType()"):
            details['column'] = column
            attributeSummary['dataType'] = "Alphanumeric"

        details['attributeSummary'] = attributeSummary
        ListResult.append(details)
    input_dict['profile'] = ListResult

    attributeSummary = get_attributeSummary()
    frequencyAnalysis = get_frequencyAnalysis()
    maskAnalysis = get_maskAnalysis()
    patternAnalysis = get_patternAnalysis()
    outliersList = get_outliersList()
    outliersPercent = get_outliersPercent()
    correlationSummary = get_correlationSummary()
    dq = get_dq()
    lengthValueStatics = get_value_lengthStatics()
    lengthStatics = get_lengthStatics()
    valueStatistics = get_valueStatics()

    






