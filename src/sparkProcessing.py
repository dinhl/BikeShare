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
    s3_buckets_list = ['citibikenydata', 'capitalbikesharedata']
    # s3_file_name = '/*.csv'
    column_name = \
        {\
        'citibikenydata':\
            {\
            'company_name': 'CitiBike_New_York',\
            'start_date': 'starttime',\
            'member_type': 'usertype',\
            'duration' : 'tripduration',\
            'start_station' : 'start station id',\
            'end_station': 'end station id',\
            'bike_number' : 'bikeid'\
            },\
        'capitalbikesharedata':\
            {\
            'company_name': 'Capital_BikeShare_Metro_DC',\
            'start_date': 'Start date',\
            'member_type': 'Member type',\
            'duration' : 'Duration',\
            'start_station' : 'Start station number',\
            'end_station': 'End station number',\
            'bike_number' : 'Bike number'\
            }\
        }

    for bucket in s3_buckets_list:
        if bucket == 'citibikenydata':
            s3_file_name = '/201605-citibike-tripdata.csv'
        else:
            s3_file_name = '/2010-capitalbikeshare-tripdata.csv'

        s3_bucket_name = bucket
        s3_url = s3_prefix + s3_bucket_name + s3_file_name
        df = spark.read.csv(s3_url, header=True, inferSchema=True)
        df.createOrReplaceTempView("sql_view")
        processed_data = spark.sql(\
            "SELECT '{0}' as company_name,\
                date_trunc('hour', `{1}`) as ride_date,\
                (dayofweek(date_trunc('day', `{1}`))) as ride_dow,\
                case when `{2}` = 'Subscriber' then 'Member' when `{2}` = 'Customer' then 'Casual'\
                else `{2}` end as member,\
                count(*) as total_rides,\
                (sum(case when `{3}` <= (30*60) then 1 else 0 end)) as total_rides_under_30min,\
                (sum(case when `{3}` > (30*60) then 1 else 0 end)) as total_rides_greater_30min,\
                int(round(sum(`{3}`/3600),0)) as total_duration_hours,\
                (count(distinct(`{4}`)) + count(distinct(`{5}`))) as total_stations,\
                count(distinct(`{6}`)) as total_bikes\
            FROM sql_view\
            GROUP BY\
                date_trunc('hour', `{1}`),\
                (dayofweek(date_trunc('day', `{1}`))),\
                `{2}`\
            ORDER BY\
                (date_trunc('hour', `{1}`)),\
                (dayofweek(date_trunc('day', `{1}`))),\
                `{2}`"\
                .format(\
                column_name[s3_bucket_name]['company_name'],\
                column_name[s3_bucket_name]['start_date'],\
                column_name[s3_bucket_name]['member_type'],\
                column_name[s3_bucket_name]['duration'],\
                column_name[s3_bucket_name]['start_station'],\
                column_name[s3_bucket_name]['end_station'],\
                column_name[s3_bucket_name]['bike_number']\
            ))

    write_to_postgres(processed_data, "postgres_data")

    spark.stop()
