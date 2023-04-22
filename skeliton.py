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
        "nr_duplicates": 0,
        "nr_totalcols": 0,
        "nr_totalrecords": 0
    }
    profile = {
        "column": ""
    }
    LengthStatistics = {
        "Min": 0,
        "Max": 0,
        "Average": 0,
        "Mean": 0
    }
    attributeSummary = {
        "records": 0,
        "dataType": "",
        "null_records": 0,
        "outliers": 0,
        "duplicates": 0,
        "invalid": 0,
    }
    correlationSummary = {
        "positiveSummary": [],
        "negativeSummary": []
    }
    datacatalogue = {
        "datacatalogue": []
    }
    frequencyAnalysis = {
        "counts": 0,
        "unique_values": 0
    }
    isFrequencyChart = {
        "isFrequencyChart": "True"
    }
    maskAnalysis = {
        "maskAnalysis": []
    }
    outliersList = {
        "outliersList": []
    }
    outliersPercent = {
        "outliers": 0,
        "normal": 100
    }
    patternAnalysis = {
        "patternAnalysis": []
    }
    staticAnalysis = {
        "count": 0,
        "DataType": "",
        "MaxLength": 0,
        "Maximumvalue": 0,
        "MeanLength": 0,
        "MeanValue": 0,
        "MedianLength": 0,
        "MedianValue": 0,
        "MinimumValue":0,
        "NullCount": 0,
        "Std_Dev": 0,
        "UniqueValuesCount": 0,
        "minLength": 0,
        "suggested_dtype": ""
    }
    valueStatistics = {
        "MaxValLength": 0,
        "MaximumValue": 0,
        "MeanValue": 0,
        "MedianValue": 0,
        "MinValLength": 0,
        "Std_Dev": 0,
        "UniquevaluesCount": 0
    }


    print(input_dict)
    return jsonify(input_dict,profile,LengthStatistics,attributeSummary,correlationSummary,datacatalogue,frequencyAnalysis,isFrequencyChart,maskAnalysis,outliersPercent,outliersList,staticAnalysis,valueStatistics)

# Convert the dictionary to a DataFrame and display it


if __name__ == '__main__':
    app.run(debug=True)



