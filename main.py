import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import min, max, avg, mean, length, col, count, corr, countDistinct, isnan, when, concat_ws
from datetime import datetime
from flask import Flask
from flask import Flask, request, render_template, send_from_directory, jsonify

app = Flask(__name__)

# Create SparkSession
spark = SparkSession.builder.appName('CSV to JSON').getOrCreate()

# Read CSV file
df = spark.read.csv('D:/AI-Data-Driven/Documents/Titanic01.csv', header=True, inferSchema=True)
df_copy = df
ref_df = df

# To list out the rows and columns of the dataframe
df.show(10)

df.printSchema()

@app.route("/")
def home():
    return ("Pyspark")

@app.route('/printSchema', methods=['GET'])
# Function to print out the schema of the dataframe, which includes data types of each column
def get_attribute_summary():
    return [df.printSchema]

@app.route('/api/profile', methods=['GET'])
def profile():
    null_value = {col: df.filter(df[col].isNull()).count() for col in df.columns}
    min_value = [df.select(F.min(column)).collect()[0][0] for column in df.columns]
    max_value = [df.select(F.max(column)).collect()[0][0] for column in df.columns]
    avg_value = [df.select(F.avg(column)).collect()[0][0] for column in df.columns]
    mean_value = [df.select(F.mean(column)).collect()[0][0] for column in df.columns] 

    values = null_value,min_value,max_value,avg_value,mean_value
    return [values]

@app.route('/api/length', methods=['GET'])
def get_length():

    col_lengths = df.select([length(c).alias(c) for c in df.columns])

    results = []
    for col in col_lengths.columns:
        row = col_lengths.agg(min(col), max(col), avg(col), mean(col)).collect()[0]
        result = {
            "column_name": col,
            "min_length": row[0],
            "max_length": row[1],
            "avg_length": row[2],
            "mean_length": row[3]
        }
        results.append(result)

    return results

@app.route("/api/frequency", methods=['GET'])
def frequencies():
    result = {}
    for column in df.columns:
        frequency_df = df.groupBy(col(column)).count()
        result[column] = frequency_df.collect()
    return result


if __name__ == '__main__':
    app.run(debug=True)



