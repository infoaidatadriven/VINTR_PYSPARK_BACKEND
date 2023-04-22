import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import min, max, avg, mean, length, col, count, corr, countDistinct, isnan, when, concat_ws
from datetime import datetime
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
    return jsonify(values)

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

    return jsonify(results)

@app.route("/api/frequency", methods=['GET'])
def frequencies():
    result = {}
    for column in df.columns:
        frequency_df = df.groupBy(col(column)).count()
        result[column] = frequency_df.collect()
    return jsonify(result)

# @app.route("/api/correlation", methods=['GET'])
# def calculate_correlations():
#     # Select only columns with numeric data types
#     numeric_cols = [col_name for col_name, col_type in df_copy.dtypes
#                     if isinstance(col_type, (FloatType, IntegerType, DoubleType))]
#
#     # Convert selected columns to float data type
#     for col_name in numeric_cols:
#         dff = df_copy.withColumn(col_name, F.col(col_name).cast("float"))
#
#     # Calculate the correlation value of each column with all other columns
#     correlations = {}
#     for col_name in numeric_cols:
#         if col_name != "id":
#             corr_values = {}
#             for other_col in numeric_cols:
#                 if other_col != col_name:
#                     corr_val = df.stat.corr(col_name, other_col)
#                     corr_values[other_col] = corr_val
#             correlations[col_name] = corr_values
#
#     # Create a Pandas DataFrame to display the correlation values
#     correlation_dff = pd.DataFrame(correlations)
#     correlation_dff.index.name = "Column"
#     correlation_dff = correlation_dff.stack().reset_index()
#     correlation_dff.columns = ["Column 1", "Column 2", "Correlation Value"]
#
#     # Convert the DataFrame to a dictionary and return it
#     return correlation_dff.to_dict(orient="records")



if __name__ == '__main__':
    app.run(debug=True)



