from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("BikeShare")\
        .getOrCreate()

    log_of_songs = [
        "Despacito",
        "Nice for what"
        ]

    for x in log_of_songs:
	print(x)

    df = spark.read.csv('s3a://capitalbikesharedata/2010-capitalbikeshare-tripdata.csv')
    df.persist()
    print(df.head(2))
    print(df.take(5))
    print(df.select("_c1","_c2","_c3","_c5","_c7","_c8").show())
    spark.stop()
