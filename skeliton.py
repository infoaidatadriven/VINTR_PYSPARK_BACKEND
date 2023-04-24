import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from datetime import datetime
from flask import Flask, request, render_template, send_from_directory, jsonify

app = Flask(__name__)

spark = SparkSession.builder.appName('CSV to JSON').getOrCreate()

df = spark.read.csv('D:/AI-Data-Driven/Documents/Titanic01.csv', header=True, inferSchema=True)

@app.route("/api/profile", methods=['GET'])
def profile():
    input_dict = {
        "profile": [
            {
                "column": "",
                "attributeSummary":
                    {
                    "records": 0,
                    "dataType": "Numeric",
                    "null_records": 0,
                    "outliers": 0,
                    "duplicates": 0,
                    "invalid": 0
                    },
                "valueStatics":
                    {
                        "MinimumValue":0,
                        "MinValLength":0,
                        "MaximumValue":0,
                        "MaxValLength":0,
                        "MeanValue":0,
                        "MedianValue":0,
                        "UniqueValuesCount":0,
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
                      "unique_values":0,
                      "counts":0
                    },
                "patternAnalysis": [],
                "maskAnalysis": [],
                "staticalAnalysis":
                    {
                        "Count": 0,
                        "NullCount": 0,
                        "UniqueValuesCount": 0,
                        "MinimumValue": 0,
                        "MeanValue": 0,
                        "MedianValue": 0,
                        "MaximumValue": 0,
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
        "nr_duplicates": 0,
        "nr_totalrecords": 0,
        "nr_totalcols": 0
    }

    return jsonify(input_dict)

if __name__ == '__main__':
    app.run(debug=True)











