import findspark

from Datacatlog import getcatalogueforcolumns

findspark.init()
from pyspark.sql import SparkSession
import json

import pyspark.sql.functions as F
from pyspark.sql.functions import col, length, min, max, avg, mean, count
from pyspark.sql.functions import col, count, when, isnan, min, max, mean, stddev, approx_count_distinct, length, approx_count_distinct, stddev
import numpy as np

from pyspark.sql.types import FloatType
from datetime import datetime
from flask import Flask,Blueprint, request, render_template, send_from_directory, jsonify
from dataLineage import EntryDataLineage ,dataLineageforcolumn
from dataQuality import checkQuality,checkDataQuality,savequlaitychecktodb



#app = Flask(__name__, static_folder='static', template_folder='static')
profile_api = Blueprint('profile_api',__name__)

spark = SparkSession.builder.appName('CSV to JSON').getOrCreate()


# @app.route("/")
# def helloWorld():
#   return render_template('index.html')
#
# @app.route("/static/")
# def helloWorld1():
#   return render_template('index.html')
# @app.route("/", methods=['GET'])
# def home():
#     return ("Pyspark ")

@profile_api.route("/api/profile", methods=['POST'])
def profile():

    content = request.get_json()
    sourcepath = content['sourcepath']
    df = spark.read.csv(sourcepath, header=True, inferSchema=True)
    # Get the number of records in the DataFrame
    num_records = df.count()

    # Get the number of columns in the DataFrame
    num_columns = len(df.columns)

    # Get the data type of each column
    column_data_types = [(column, str(data_type)) for column, data_type in df.dtypes]



    columns = df.columns
    input_dict = {"profile": []}
    input_dict['nr_totalrecords'] = num_records
    input_dict['nr_totalcols'] = num_columns

    ListResult = []

    selectedColumns = list(df.columns)
    catalogueresults=getcatalogueforcolumns(selectedColumns)
    print("Starting the qulaity check")
    #qualityresult = checkDataQuality(df.head(1000), selectedColumns)
    print("completed the qulaity check")
    #c = savequlaitychecktodb(qualityresult, sourcepath)

    print(selectedColumns)

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



        #num_duplicates = df.select(column).distinct().count() - df.select(column).count()
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
        medians = np.median(lengths.toPandas(), axis=0)
        median_stats = dict(zip(lengths.columns, medians))



        attributeSummary['duplicates'] = 0
        attributeSummary['null_records'] = null_records
        attributeSummary['records'] = num_records
        attributeSummary['outliers'] = 0
        attributeSummary['invalid'] = 0

        frequencyAnalysis['frequencyAnalysis'] = []
        details['frequencyAnalysis'] = frequencyAnalysis

        maskAnalysis['maskAnalysis'] = []
        details['maskAnalysis'] = maskAnalysis

        patternAnalysis['patternAnalysis'] = []
        details['patternAnalysis'] = patternAnalysis

        outliersList['outliersList'] = []
        details['outliersList'] = outliersList

        outliersPercent['outliersPercent'] = {}
        details['outliersPercent'] = outliersPercent

        correlationSummary['positiveSummary'] = 0
        correlationSummary['negativeSummary'] = 0
        details['correlationSummary'] = correlationSummary

        dq['dq'] = 0
        details['dq'] = dq


        LengthStatics['Min'] = int(length_stats.first()["Min_" + column])
        LengthStatics['Max'] = int(length_stats.first()["Max_" + column])
        LengthStatics['Average'] = round(length_stats.first()["Average_" + column], 2)
        LengthStatics['Median'] = int(median_stats[column])
        details['LengthStatics'] = LengthStatics



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
        staticalAnalysis['MedianLength'] = int(median_stats[column])
        staticalAnalysis['Data Type'] = data_type
        staticalAnalysis['suggested_dtype'] = ""
        details['staticAnalysis'] = staticalAnalysis





        catalogueresult = [x for x in catalogueresults if x['Column'] == column]
        dcatres = []
        for eachresult in catalogueresult:
            eachresult['datalineage'] = dataLineageforcolumn(column)
            dcatres.append(eachresult)
        details['datacatalogue'] = dcatres
        #details['dq'] = [x for x in qualityresult if x['ColumnName'] == column][0]



        if (data_type=="IntegerType()"  or data_type=="DoubleType()"):
            details['column'] = column
            attributeSummary['dataType'] = "Numeric"
            valueStatistics["MinimumValue"] = df.select(F.min(column)).collect()[0][0]
            valueStatistics["MinValLength"] = int(length_stats.first()["Min_" + column])
            valueStatistics["MaximumValue"] = df.select(F.max(column)).collect()[0][0]
            valueStatistics["MaxValLength"] = int(length_stats.first()["Max_" + column])
            valueStatistics["MeanValue"] = df.select(F.mean(column)).collect()[0][0]
            valueStatistics["MedianValue"] = df.select(F.avg(column)).collect()[0][0]
            valueStatistics["UniqueValuesCount"] = unique_values_count
            valueStatistics["Std_Dev"] = std_dev
            details['valueStatistics'] = valueStatistics



        if (data_type=="datetime64[ns]" or data_type=="datetime"):
            details['column']=column
            attributeSummary['dataType'] = "Alphabetic"
        if (data_type=="StringType()"):
            details['column']=column
            attributeSummary['dataType'] = "Alphanumeric"

        details['attributeSummary'] = attributeSummary
        ListResult.append(details)
    input_dict['profile'] = ListResult

    # Print the input_dict containing the profile information
    jsonString = json.dumps(input_dict, default=str)
    return jsonString


if __name__ == '__main__':
    app.run(debug=True)











