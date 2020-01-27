from __future__ import print_function
import os

from pyspark.sql import DataFrameWriter
from pyspark.sql import DataFrameReader

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

    pg_hostName = os.environ["PG_HOST"]
    pg_user = os.environ["PG_USER"]
    pg_password = os.environ["PG_PASSWORD"]
    pg_databaseName = os.environ["PG_DATABASE_NAME"]

    pg_url = "jdbc:postgresql://" + pg_hostName + ":5432/" + pg_databaseName

    df.write.jdbc(url=pg_url, table="sparkData", properties={"user": pg_databaseName, "password": pg_password, "driver": "org.postgresql.Driver"})

    spark.stop()