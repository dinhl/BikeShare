import os
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *


def write_to_postgres(df, table_name):

    pg_host = os.environ['PG_HOST']
    pg_database = 'bikeshare'
    pg_url = 'jdbc:postgresql://' + pg_host + ':5432/' + pg_database
    pg_properties = {
        'user': os.environ['PG_USER'],
        'password': os.environ['PG_PASS'],
        'driver': 'org.postgresql.Driver'
    }

    df.write.jdbc(url=pg_url, table=table_name, mode="append", properties=pg_properties)

if __name__ == "__main__":

    spark = SparkSession.builder.appName("BikeShare").getOrCreate()

    s3_prefix = 's3a://'
    s3_bucket_capital_bike = 'capitalbikesharedata'
    s3_file_name = '/*.csv'

    s3_url_capital_bike = s3_prefix + s3_bucket_capital_bike + s3_file_name

    df_capital_bike = spark.read.csv(s3_url_capital_bike, header=True, inferSchema=True)

    df_capital_bike.createOrReplaceTempView("sql_view_capital")

    df_output = spark.sql("SELECT 'CapitalBike' as company_name,\
            date_trunc('day', `Start Date`) as ride_date,\
            (dayofweek(date_trunc('day', `Start Date`))) as ride_DOW,\
            `Member type` as member,\
            count(*) as total_rides,\
            (sum(case when `Duration` <= (30*60) then 1 else 0 end)) as total_rides_under_30min,\
            (sum(case when `Duration` > (30*60) then 1 else 0 end)) as total_rides_greater_30min,\
            int(round(sum((`Duration`)/3600),0)) as total_duration_minutes,\
            (count(distinct(`Start station number`)) + count(distinct(`End station number`))) as total_stations,\
            count(distinct(`Bike number`)) as total_bikes\
        	FROM sql_view_capital\
        	GROUP BY date_trunc('day', `Start Date`),\
            (dayofweek(date_trunc('day', `Start Date`))),\
            `Member type`\
        	ORDER BY (date_trunc('day', `Start Date`)),\
            `Member type`")

    write_to_postgres(df_output, "capital")

    spark.stop()