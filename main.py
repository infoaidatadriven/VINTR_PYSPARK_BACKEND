import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, length, min, max, avg, mean
from pyspark.sql.types import FloatType
from datetime import datetime
from flask import Flask, request, render_template, send_from_directory, jsonify

app = Flask(__name__)

spark = SparkSession.builder.appName('CSV to JSON').getOrCreate()

df = spark.read.csv('D:/AI-Data-Driven/Documents/Titanic01.csv', header=True, inferSchema=True)

@app.route("/", methods=['GET'])
def home():
    return ("Pyspark ")

@app.route("/api/profile", methods=['GET'])
def profile():

    # Get the number of records in the DataFrame
    num_records = df.count()

    # Get the number of columns in the DataFrame
    num_columns = len(df.columns)

    # Get the data type of each column
    column_data_types = [(column, str(data_type)) for column, data_type in df.dtypes]

    # Get Length statics


    # Get the total number of duplicate values for each column
    duplicates = []
    for column in df.columns:
        num_duplicates = df.groupBy(column).count().filter(col("count") > 1).count()
        duplicates.append((column, num_duplicates))


    input_dict = {
        "profile": [
            {
                "column": "",
                "attributeSummary":
                    {
                    "records": num_records,
                    "dataType": "",
                    "null_records": {col: df.filter(df[col].isNull()).count() for col in df.columns},
                    "outliers": 0,
                    "duplicates": num_duplicates,
                    "invalid": 0
                    },
                "valueStatics":
                    {
                        "MinimumValue": [df.select(F.min(column)).collect()[0][0]],
                        "MinValLength":0,
                        "MaximumValue": [df.select(F.max(column)).collect()[0][0]],
                        "MaxValLength":0,
                        "MeanValue": [df.select(F.mean(column)).collect()[0][0]],
                        "MedianValue":[df.select(F.avg(column)).collect()[0][0]],
                        "UniqueValuesCount": 0,
                        "Std_Dev":0
                    },
                "LengthStatistics":
                    {
                        "Min": 0,
                        "Max": 0,
                        "Average": 0,
                        "Median": 0
                    },
                "frequncyAnalysis":
                    {
                      "unique_values": 0,
                      "counts": 0
                    },
                "patternAnalysis": [],
                "maskAnalysis": [],
                "staticalAnalysis":
                    {
                        "Count": 0,
                        "NullCount": 0,
                        "UniqueValuesCount": 0,
                        "MinimumValue": [df.select(F.min(column)).collect()[0][0]],
                        "MeanValue": [df.select(F.mean(column)).collect()[0][0]],
                        "MedianValue": 0,
                        "MaximumValue": [df.select(F.max(column)).collect()[0][0]],
                        "Std_Dev": 0,
                        "minLength": 0,
                        "MaxLength": 0,
                        "MeanLength": 0,
                        "MedianLength": 0,
                        "Data Type": "",
                        "suggested_dtype": ""
                    },
                "outliersList": [],
                "outliersPercent":
                    {
                        "outliers": 0,
                        "normal": 0
                    },
                "isFrequencyChart": "",
                "correlaionSummary":
                    {
                        "positiveSummary":
                            [
                                {
                                    "column": "",
                                    "value": ""
                                }
                            ],
                        "negativeSummary": []
                    },
                "datacatalogue": [
                    {
                        "Column": "",
                        "id": "",
                        "BusinessTerm": "",
                        "Definition": "",
                        "Classification": "",
                        "DataDomain": "",
                        "RelatedTerms": "",
                        "RelatedSource": "",
                        "RelatedReports": "",
                        "DataOwners": "",
                        "DataUsers": "",
                        "RelatedTags": "",
                        "datalineage":[
                            {
                                "LineageName": "",
                                "Details": {
                                    "name": "",
                                    "type": "",
                                    "id": "",
                                    "metaData": [
                                        {
                                            "column": "",
                                            "id": "",
                                            "BusinessTerm": ""
                                        }
                                    ]
                                }
                            },

                        ]
                    }
                ],
                "dq": {
                    "ColumnName": "",
                    "detail": {
                        "Completeness": {
                            "value": "",
                            "info": [
                                {
                                    "rule": "",
                                    "OutlierCount": ""
                                }
                            ]
                        },
                        "Validity": {
                            "value": "",
                            "info": [
                                {
                                    "rule": "DataType match check (DataType : Numeric) ",
                                    "OutlierCount": "0"
                                },
                                {
                                    "rule": "Length check (Min Length should be 3) ",
                                    "OutlierCount": "0"
                                },
                                {
                                    "rule": "Length check (Max Length should be 4) ",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Consistenecy": {
                            "value": "",
                            "info": [
                                {
                                    "rule": "No Rules Executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Timeliness": {
                            "value": "",
                            "info": [
                                {
                                    "rule": "file reception frequency check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Accuracy": {
                            "value": "",
                            "info": [
                                {
                                    "rule": "No Rules executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        }
                    },
                    "overall": ""
                }
            },

        ],
        "nr_duplicates": num_duplicates,
        "nr_totalrecords": num_records,
        "nr_totalcols": num_columns
    }

    return jsonify(input_dict)

if __name__ == '__main__':
    app.run(debug=True)











