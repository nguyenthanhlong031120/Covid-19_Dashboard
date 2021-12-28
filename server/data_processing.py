from pyspark.sql.functions import sum, col, desc
from flask import Response
import json
import pyspark
from pyspark.sql import SparkSession


import pandas as pd


import findspark
findspark.init()


spark = SparkSession.builder.master("local[1]").appName(
    "SparkByExamples.com").getOrCreate()

dataset = spark.read.csv('server/data/WHO-COVID-19-global-data.csv',
                         inferSchema=True, header=True)


def global_death(dataset):
    sumdeath = dataset.groupBy("Date_reported").agg(
        sum("New_deaths").alias("Deaths")).sort(desc("Date_reported"))
    pandasDF2 = sumdeath.toPandas()
    result = {}
    for index, row in pandasDF2.iterrows():
        #result[index] = row.to_json()
        result[index] = dict(row)
    return result


global_deaths = global_death(dataset)

print(global_deaths)
