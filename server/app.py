import findspark

findspark.init()

import pandas as pd
from flask import Flask, jsonify
from pyspark.sql import SparkSession
import pyspark
import json
from flask import Response
from pyspark.sql.functions import sum, col, desc
from flask_cors import CORS

spark = SparkSession.builder.master("local[1]").appName(
    "SparkByExamples.com").getOrCreate()

app = Flask(__name__)
CORS(app)

dataset = spark.read.csv('data/WHO-COVID-19-global-data.csv',
                            inferSchema=True, header=True)

# Hello world API Route
@app.route("/", methods=["GET"])
def index():
    return "Hello world!"


# Global
#Tổng số lượng ca mắc và ca tử vong trên toàn thế giới
@app.route("/global", methods=["GET"])
def global_sum_case_death():
    Global_sum_case_death = dataset.groupBy("Date_reported").agg(sum("Cumulative_cases").alias("Cases") , sum("Cumulative_deaths").alias("Deaths")).filter(dataset.Date_reported == "2021-10-28")
    result = json.dumps(Global_sum_case_death.toJSON().map(lambda j: json.loads(j)).collect())
    return result


#Danh sách ca mắc và ca tử vong trên toàn thế giới trong vòng 30 ngày
@app.route("/global30days", methods=["GET"])
def global_case_death():
    Global_case_death = dataset.groupBy("Date_reported").agg(sum("New_cases").alias("Cases") 
        , sum("New_deaths").alias("Deaths")).filter(dataset.Date_reported > "2021-09-28").sort(dataset.Date_reported)
    result = json.dumps(Global_case_death.toJSON().map(lambda j: json.loads(j)).collect())
    return result


# list region
@app.route("/regions", methods=["GET"])
def regions():
    regions = dataset.select('WHO_region').distinct().sort("WHO_region")
    result = json.dumps(regions.toJSON().map(lambda j: json.loads(j)).collect())
    return result



#Tổng ca mắc theo từng khu vực
@app.route("/region", methods=["GET"])
def region_sum():
    Regions_cases_deaths = dataset.filter(dataset.Date_reported == "2021-10-28")
    Regions_case_death= Regions_cases_deaths.groupBy("WHO_region").agg(
        sum("Cumulative_cases").alias("Cases"), sum("Cumulative_deaths").alias("Deaths"))

    result = json.dumps(Regions_case_death.toJSON().map(lambda j: json.loads(j)).collect())
    return result


#Tổng ca mắc theo từng khu vực theo ngày
@app.route("/region/<string:region>/<string:date_from>/<string:date_to>", methods=["GET"])
def region_case_death_date(region,date_from,date_to):
    region_case_death_filterdate = dataset.groupBy("Date_reported", "WHO_region").agg(sum("New_cases").alias("Cases") , 
        sum("New_deaths").alias("Deaths")).filter(
            (dataset.Date_reported > date_from ) &  
            (dataset.Date_reported < date_to ) & 
            (dataset.WHO_region == region ) )

    result = json.dumps(region_case_death_filterdate.toJSON().map(lambda j: json.loads(j)).collect())
    return result


# list country
@app.route("/countrys", methods=["GET"])
def countrys():
    countrys = dataset.select('Country', "Country_code").distinct().sort("Country")
    result = json.dumps(countrys.toJSON().map(lambda j: json.loads(j)).collect())
    return result


#Tổng ca mắc theo từng khu vực theo ngày
@app.route("/country/<string:Country_code>/<string:date_from>/<string:date_to>", methods=["GET"])
def country_case_death(Country_code,date_from,date_to):
    country_case_death_filterdate = dataset.groupBy("Date_reported", "Country", "Country_code").agg(sum("New_cases").alias("Cases") , 
        sum("New_deaths").alias("Deaths")).filter(
            (dataset.Date_reported > date_from ) &  
            (dataset.Date_reported < date_to ) & 
            (dataset.Country_code == Country_code ) )

    result = json.dumps(country_case_death_filterdate.toJSON().map(lambda j: json.loads(j)).collect())
    return result

#Danh sách ngày
@app.route("/dates", methods=["GET"])
def date():
    dates = dataset.select('Date_reported').distinct().sort("Date_reported")
    result = json.dumps(dates.toJSON().map(lambda j: json.loads(j)).collect())
    return result




if __name__ == "__main__":
    app.run(debug=True)
