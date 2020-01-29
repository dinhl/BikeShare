import os
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit


def write_to_postgres(df, table):

    pg_host = os.environ['PG_HOST']
    pg_database = 'bikeshare'
    pg_url = 'jdbc:postgresql://' + pg_host + ':5432/' + pg_database
    pg_properties = {
        'user': os.environ['PG_USER'],
        'password': os.environ['PG_PASS'],
        'driver': 'org.postgresql.Driver'
    }

    df.write.jdbc(url=pg_url, table=table, mode="overwrite", properties=pg_properties)


if __name__ == "__main__":

    spark = SparkSession.builder.appName("BikeShare").getOrCreate()

    s3 = "s3a://capitalbikesharedata/2010-capitalbikeshare-tripdata.csv"

    df = spark.read.csv(s3, header=True)

    df.createOrReplaceTempView("sql_view")

    results = spark.sql("SELECT `Start Date`, `End Date`, `Duration`, `Start station number`, `End station number`, `Bike number`, `Member type` FROM sql_view")

    name = "Bike"
    data = results.withColumn("CompanyName", lit(name))
    write_to_postgres(data, "Bike")

    spark.stop()
