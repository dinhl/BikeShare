import os
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import types


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

def add_column_if_not_exist(df, col_name, dtype):
    return (df if col_name in df.columns
        else df.withColumn(col_name, lit(None).cast(dtype)))

if __name__ == "__main__":

    spark = SparkSession.builder.appName("BikeShare").getOrCreate()
    

    s3_prefix = 's3a://'
    

    s3_buckets_list = [\
    'citibikenydata', 'capitalbikesharedata', 'bluebike-bikeshare-data',\
    'chattanooga-bikeshare-data', 'cogo-bikeshare-data', 'divvy-bikeshare-data',\
    'healthyride-bikeshare-data', 'indegodata', 'niceride-bikeshare-data', 'metrobikes-bikeshare-data']

    s3_file_name = '/*.csv'
    

    column_name = \
    {\
        'citibikenydata':\
        {\
            'company_name': 'CitiBike_NewYork',\
            'start_date': 'starttime',\
            'member_type': 'usertype',\
            'duration' : 'tripduration',\
            'start_station' : 'start station id',\
            'end_station': 'end station id',\
            'bike_number' : 'bikeid'\
        },\
        'capitalbikesharedata':\
        {\
            'company_name': 'Capital_BikeShare_MetroDC',\
            'start_date': 'Start date',\
            'member_type': 'Member type',\
            'duration' : 'Duration',\
            'start_station' : 'Start station number',\
            'end_station': 'End station number',\
            'bike_number' : 'Bike number'\
        },\
        'bluebike-bikeshare-data':\
        {\
            'company_name': 'BlueBikes_Boston_MA',\
            'start_date': 'starttime',\
            'member_type': 'usertype',\
            'duration' : 'tripduration',\
            'start_station' : 'start station id',\
            'end_station': 'end station id',\
            'bike_number' : 'bikeid'\
        },\
        'chattanooga-bikeshare-data':\
        {\
            'company_name': 'Chatanooga_TN',\
            'start_date': 'Start Time',\
            'member_type': 'Member Type',\
            'duration' : 'TripDurationMin',\
            'start_station' : 'Start Station ID',\
            'end_station': 'End Station ID',\
            'bike_number' : 'BikeID'\
        },\
        'cogo-bikeshare-data':\
        {\
            'company_name': 'Cogo_Columbus_OH',\
            'start_date': 'start_time',\
            'member_type': 'usertype',\
            'duration' : 'tripduration',\
            'start_station' : 'from_station_id',\
            'end_station': 'to_station_id',\
            'bike_number' : 'bikeid'\
        },\
        'divvy-bikeshare-data':\
        {\
            'company_name': 'Divvy_Chicago_IL',\
            # 'start_date': 'start_time',\
            'start_date': 'starttime',\
            'member_type': 'usertype',\
            'duration' : 'tripduration',\
            'start_station' : 'from_station_id',\
            'end_station': 'to_station_id',\
            'bike_number' : 'bikeid'\
        },\
        'healthyride-bikeshare-data':\
        {\
            'company_name': 'HealthyRide_Pittsburgh_PA',\
            'start_date': 'Starttime',\
            'member_type': 'Usertype',\
            'duration' : 'Tripduration',\
            'start_station' : 'From station id',\
            'end_station': 'To station id',\
            'bike_number' : 'Bikeid'\
        },\
        'indegodata':\
        {\
            'company_name': 'Indego_Philadelphia_PA',\
            'start_date': 'start_time',\
            'member_type': 'passholder_type',\
            'duration' : 'duration',\
            'start_station' : 'start_station',\
            'end_station': 'end_station',\
            'bike_number' : 'bike_id'\
        },\
        'niceride-bikeshare-data':\
        {\
            'company_name': 'NiceRide_Minneapolis_MN',\
            'start_date': 'start_time',\
            'member_type': 'usertype',\
            'duration' : 'tripduration',\
            'start_station' : 'start station id',\
            'end_station': 'end station id',\
            'bike_number' : 'bikeid'\
        },\
        'metrobikes-bikeshare-data':\
        {\
            'company_name': 'MetroBikes_LA_CA',\
            'start_date': 'start_time',\
            'member_type': 'passholder_type',\
            'duration' : 'duration',\
            'start_station' : 'start_station',\
            'end_station': 'end_station',\
            'bike_number' : 'bike_id'\
        }\
        }


    for bucket in s3_buckets_list:
        
        s3_bucket_name = bucket
        
        s3_url = s3_prefix + s3_bucket_name + s3_file_name
        df = spark.read.csv(s3_url, header=True, inferSchema=True, timestampFormat="MM/dd/yyyy HH:mm")
        

        df.createOrReplaceTempView("sql_view")
        processed_data = spark.sql(\
            "SELECT '{0}' as company_name,\
                date_trunc('hour', `{1}`) as ride_date,\
                (dayofweek(date_trunc('day', `{1}`))) as ride_dow,\
                case when `{2}` = 'Subscriber' then 'Member' \
                when `{2}` = 'Indego365' then 'Member' \
                when `{2}` = 'MonthlyPass' then 'Member' \
                when `{2}` = 'Customer' then 'Casual'\
                when `{2}` = 'Day Pass' then 'Casual'\
                when `{2}` = 'Walk-up' then 'Casual'\
                else `{2}` end as member,\
                count(*) as total_rides,\
                (sum(case when `{3}` <= (30*60) then 1 else 0 end)) as total_rides_under_30min,\
                (sum(case when `{3}` > (30*60) then 1 else 0 end)) as total_rides_greater_30min,\
                int(round(sum(`{3}`/3600),0)) as total_duration_hours,\
                (count(distinct(`{4}`)) + count(distinct(`{5}`))) as total_stations,\
                count(distinct(`{6}`)) as total_bikes, '{7}' as file_name\
            FROM sql_view\
            GROUP BY\
                date_trunc('hour', `{1}`),\
                (dayofweek(date_trunc('day', `{1}`))),\
                `{2}`\
            ORDER BY\
                (date_trunc('hour', `{1}`)),\
                (dayofweek(date_trunc('day', `{1}`))),\
                `{2}`".format(\
                column_name[s3_bucket_name]['company_name'],\
                column_name[s3_bucket_name]['start_date'],\
                column_name[s3_bucket_name]['member_type'],\
                column_name[s3_bucket_name]['duration'],\
                column_name[s3_bucket_name]['start_station'],\
                column_name[s3_bucket_name]['end_station'],\
                column_name[s3_bucket_name]['bike_number'], s3_file_name\
            ))
        
        processed_data.show()
        write_to_postgres(processed_data, "postgresql_data")

    spark.stop()
