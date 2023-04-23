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
                "column": "PassengerId",
                "attributeSummary":
                    {
                    "records": 418,
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
                    "ColumnName": "PassengerId",
                    "detail": {
                        "Completeness": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "Null check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Validity": {
                            "value": "100.0",
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
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "No Rules Executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Timeliness": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "file reception frequency check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Accuracy": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "No Rules executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        }
                    },
                    "overall": "100.0"
                }
            },
            {
                "column": "Pclass",
                "attributeSummary": {
                    "records": 418,
                    "dataType": "Numeric",
                    "null_records": 0,
                    "outliers": 0,
                    "duplicates": 0,
                    "invalid": 0
                },
                "valueStatistics": {
                    "MinimumValue": 1,
                    "MinValLength": 1,
                    "MaximumValue": 3,
                    "MaxValLength": 1,
                    "MeanValue": 2.27,
                    "MedianValue": 3,
                    "UniqueValuesCount": 3,
                    "Std_Dev": 0.84
                },
                "LengthStatistics": {
                    "Min": 1,
                    "Max": 1,
                    "Average": 1,
                    "Median": 1
                },
                "frequncyAnalysis": [
                    {
                        "unique_values": 3,
                        "counts": 218
                    },
                    {
                        "unique_values": 1,
                        "counts": 107
                    },
                    {
                        "unique_values": 2,
                        "counts": 93
                    }
                ],
                "patternAnalysis": [],
                "maskAnalysis": [],
                "staticalAnalysis": {
                    "Count": 418,
                    "NullCount": 0,
                    "UniqueValuesCount": 3,
                    "MinimumValue": 1,
                    "MeanValue": 2.27,
                    "MedianValue": 3,
                    "MaximumValue": 3,
                    "Std_Dev": 0.84,
                    "minLength": 1,
                    "MaxLength": 1,
                    "MeanLength": 1,
                    "MedianLength": 1,
                    "Data Type": "Numeric",
                    "suggested_dtype": "Numeric"
                },
                "outliersList": [],
                "outliersPercent": {
                    "outliers": 0,
                    "normal": 100
                },
                "isFrequencyChart": "True",
                "correlationSummary": {
                    "positiveSummary": [
                        {
                            "column": "Parch",
                            "value": 0.02
                        },
                        {
                            "column": "SibSp",
                            "value": 0
                        }
                    ],
                    "negativeSummary": []
                },
                "datacatalogue": [
                    {
                        "Column": "Pclass",
                        "id": "c2",
                        "BusinessTerm": "Ticket class",
                        "Definition": " 1 = 1st, 2 = 2nd, 3 = 3rd ; A proxy for socio-economic status (SES) 1st = Upper,2nd = Middle,3rd = Lower",
                        "Classification": "Categorial",
                        "DataDomain": "Travel",
                        "RelatedTerms": "",
                        "RelatedSource": "",
                        "RelatedReports": "",
                        "DataOwners": "Vinoth",
                        "DataUsers": "Admin",
                        "RelatedTags": "Classification",
                        "datalineage": [
                            {
                                "LineageName": "File Storage",
                                "Details": {
                                    "name": "S2S1Titanic.xls",
                                    "type": "File",
                                    "id": "S2",
                                    "metaData": [
                                        {
                                            "Column": "Pclass",
                                            "id": "c2",
                                            "BusinessTerm": "Ticket class"
                                        }
                                    ]
                                }
                            },
                            {
                                "LineageName": "Extract",
                                "Details": {
                                    "name": "S2S1Titanic.xls",
                                    "type": "Dataset",
                                    "metaData": [
                                        {
                                            "Column": "Pclass",
                                            "id": "c2",
                                            "BusinessTerm": "Ticket class"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ],
                "dq": {
                    "ColumnName": "Pclass",
                    "detail": {
                        "Completeness": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "Null check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Validity": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "DataType match check (DataType : Numeric) ",
                                    "OutlierCount": "0"
                                },
                                {
                                    "rule": "Length check (Max Length should be 1) ",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Consistenecy": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "No Rules Executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Timeliness": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "file reception frequency check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Accuracy": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "No Rules executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        }
                    },
                    "overall": "100.0"
                }
            },
            {
                "column": "Embarked",
                "attributeSummary": {
                    "records": 418,
                    "dataType": "Alphabetic",
                    "null_records": 0,
                    "outliers": 0,
                    "duplicates": 0,
                    "invalid": 0
                },
                "LengthStatistics": {
                    "Min": 1,
                    "Max": 1,
                    "Average": 1,
                    "Median": 1
                },
                "frequncyAnalysis": [
                    {
                        "unique_values": "S",
                        "counts": 270
                    },
                    {
                        "unique_values": "C",
                        "counts": 102
                    },
                    {
                        "unique_values": "Q",
                        "counts": 46
                    }
                ],
                "patternAnalysis": [
                    {
                        "unique_values": "W",
                        "counts": 418
                    }
                ],
                "maskAnalysis": [
                    {
                        "unique_values": "L",
                        "counts": 418
                    }
                ],
                "staticalAnalysis": {
                    "Count": 418,
                    "UniqueValuesCount": 3,
                    "MinimumLength": 1,
                    "MaximumLength": 1,
                    "DataType": "string",
                    "Alphabetic": 418,
                    "Numeric": 0,
                    "Alphanumeric": 0,
                    "NullCount": 0,
                    "MeanLength": 1,
                    "MedianLength": 1,
                    "duplicateCount": 415,
                    "PercentageAlpha": 100,
                    "PercentageNum": 0,
                    "PercentageAlNum": 0,
                    "suggested_dtype": "Alphabetic"
                },
                "outliersList": [],
                "outliersPercent": {},
                "isFrequencyChart": "True",
                "correlationSummary": {
                    "positiveSummary": [
                        {
                            "column": "Ticket",
                            "value": 1
                        },
                        {
                            "column": "Name",
                            "value": 1
                        },
                        {
                            "column": "Cabin",
                            "value": 0.98
                        },
                        {
                            "column": "Sex",
                            "value": 0.01
                        }
                    ],
                    "negativeSummary": []
                },
                "datacatalogue": [
                    {
                        "Column": "Embarked",
                        "id": "c11",
                        "BusinessTerm": "Port of Embarkation",
                        "Definition": "C = Cherbourg, Q = Queenstown, S = Southampton",
                        "Classification": "Categorial",
                        "DataDomain": "Travel",
                        "RelatedTerms": "",
                        "RelatedSource": "",
                        "RelatedReports": "",
                        "DataOwners": "Vinoth",
                        "DataUsers": "Admin",
                        "RelatedTags": "Classification",
                        "datalineage": [
                            {
                                "LineageName": "File Storage",
                                "Details": {
                                    "name": "S2S1Titanic.xls",
                                    "type": "File",
                                    "id": "S2",
                                    "metaData": [
                                        {
                                            "Column": "Embarked",
                                            "id": "c11",
                                            "BusinessTerm": "Port of Embarkation"
                                        }
                                    ]
                                }
                            },
                            {
                                "LineageName": "Extract",
                                "Details": {
                                    "name": "S2S1Titanic.xls",
                                    "type": "Dataset",
                                    "metaData": [
                                        {
                                            "Column": "Embarked",
                                            "id": "c11",
                                            "BusinessTerm": "Port of Embarkation"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ],
                "dq": {
                    "ColumnName": "Embarked",
                    "detail": {
                        "Completeness": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "Null check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Validity": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "DataType match check (DataType : alphabets) ",
                                    "OutlierCount": "0"
                                },
                                {
                                    "rule": "Length check (Max Length should be 1) ",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Consistenecy": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "No Rules Executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Timeliness": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "file reception frequency check",
                                    "OutlierCount": "0"
                                }
                            ]
                        },
                        "Accuracy": {
                            "value": "100.0",
                            "info": [
                                {
                                    "rule": "No Rules executed",
                                    "OutlierCount": "0"
                                }
                            ]
                        }
                    },
                    "overall": "97.17"
                }
            }
        ],
        "nr_duplicates": 0,
        "nr_totalrecords": 418,
        "nr_totalcols": 11
    }

    return jsonify(input_dict)

if __name__ == '__main__':
    app.run(debug=True)











