import os
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_s3_bucket_list():
    """Return a list of bucket names in AWS S3"""

    s3_buckets_list = [
        'capitalbikesharedata', 'bluebike-bikeshare-data',
        'chattanooga-bikeshare-data', 'cogo-bikeshare-data', 
        'healthyride-bikeshare-data', 'niceride-bikeshare-data', 
        'citibikenydata', 'citibikenydata-1', 'citibikenydata-2',
        'divvy-bikeshare-data', 'divvy-bikeshare-data-1',
        'indegodata', 'indegodata-1', 'indegodata-2', 'indegodata-3',
        'metrobikes-bikeshare-data', 'metrobikes-bikeshare-data-2019',
        'metro-bikeshare-data-1'
        ]

    return s3_buckets_list


def get_company_name():
    """Get the company name for the files in different S3 bucket"""

    company_name = {
        'capitalbikesharedata': 'Capital_BikeShare_MetroDC',
        'bluebike-bikeshare-data': 'BlueBikes_Boston_MA',
        'chattanooga-bikeshare-data': 'Chatanooga_TN',
        'cogo-bikeshare-data': 'Cogo_Columbus_OH',
        'healthyride-bikeshare-data': 'HealthyRide_Pittsburgh_PA',
        'niceride-bikeshare-data': 'NiceRide_Minneapolis_MN',
        'citibikenydata': 'CitiBike_NewYork',
        'citibikenydata-1': 'CitiBike_NewYork',
        'citibikenydata-2': 'CitiBike_NewYork',
        'divvy-bikeshare-data': 'Divvy_Chicago_IL',
        'divvy-bikeshare-data-1': 'Divvy_Chicago_IL',
        'indegodata': 'Indego_Philadelphia_PA',
        'indegodata-1': 'Indego_Philadelphia_PA',
        'indegodata-2': 'Indego_Philadelphia_PA',
        'indegodata-3': 'Indego_Philadelphia_PA',
        'metrobikes-bikeshare-data': 'MetroBikes_LA_CA',
        'metrobikes-bikeshare-data-2019': 'MetroBikes_LA_CA',
        'metro-bikeshare-data-1': 'MetroBikes_LA_CA',
        }

    return company_name


def get_column_name():
    """Define column names for the extracted data stored at Postgres DB"""

    column_name = [
        'start_date', 'member_type', 'ride_duration',
        'start_station', 'end_station', 'bike_number'
        ]      
    
    return column_name


def rename_column(df, s3_bucket_name):
    """
    Rename selected columns from raw data for Spark Processing

    Parameters:
        df -- the dataframe which hold the raw data read from S3
        s3_bucket_name -- the S3 bucket name where the raw data from

    Return:
        The dataframe with consistent column names
    """

    if s3_bucket_name == 'capitalbikesharedata':            
        df = df.withColumnRenamed('Start date', 'start_date')\
               .withColumnRenamed('Member type', 'member_type')\
               .withColumnRenamed('Duration', 'ride_duration')\
               .withColumnRenamed('Start station number', 'start_station')\
               .withColumnRenamed('End station number', 'end_station')\
               .withColumnRenamed('Bike number', 'bike_number')

    if s3_bucket_name == 'bluebike-bikeshare-data':            
        df = df.withColumnRenamed('starttime', 'start_date')\
               .withColumnRenamed('usertype', 'member_type')\
               .withColumnRenamed('tripduration', 'ride_duration')\
               .withColumnRenamed('start station id', 'start_station')\
               .withColumnRenamed('end station id', 'end_station')\
               .withColumnRenamed('bikeid', 'bike_number')

    if s3_bucket_name == 'chattanooga-bikeshare-data':            
        df = df.withColumnRenamed('Start Time', 'start_date')\
               .withColumnRenamed('Member Type', 'member_type')\
               .withColumnRenamed('TripDurationMin', 'ride_duration')\
               .withColumnRenamed('Start Station ID', 'start_station')\
               .withColumnRenamed('End Station ID', 'end_station')\
               .withColumnRenamed('BikeID', 'bike_number')    

    if s3_bucket_name == 'cogo-bikeshare-data':            
        df = df.withColumnRenamed('start_time', 'start_date')\
               .withColumnRenamed('usertype', 'member_type')\
               .withColumnRenamed('tripduration', 'ride_duration')\
               .withColumnRenamed('from_station_id', 'start_station')\
               .withColumnRenamed('to_station_id', 'end_station')\
               .withColumnRenamed('bikeid', 'bike_number')    

    if s3_bucket_name == 'healthyride-bikeshare-data':            
        df = df.withColumnRenamed('Starttime', 'start_date')\
               .withColumnRenamed('Usertype', 'member_type')\
               .withColumnRenamed('Tripduration', 'ride_duration')\
               .withColumnRenamed('From station id', 'start_station')\
               .withColumnRenamed('To station id', 'end_station')\
               .withColumnRenamed('Bikeid', 'bike_number')    

    if s3_bucket_name == 'niceride-bikeshare-data':            
        df = df.withColumnRenamed('start_time', 'start_date')\
               .withColumnRenamed('usertype', 'member_type')\
               .withColumnRenamed('tripduration', 'ride_duration')\
               .withColumnRenamed('start station id', 'start_station')\
               .withColumnRenamed('end station id', 'end_station')\
               .withColumnRenamed('bikeid', 'bike_number')    

    if s3_bucket_name == 'citibikenydata' or s3_bucket_name == 'citibikenydata-1':            
        df = df.withColumnRenamed('starttime', 'start_date')\
               .withColumnRenamed('usertype', 'member_type')\
               .withColumnRenamed('tripduration', 'ride_duration')\
               .withColumnRenamed('start station id', 'start_station')\
               .withColumnRenamed('end station id', 'end_station')\
               .withColumnRenamed('bikeid', 'bike_number')    
    
    if s3_bucket_name == 'citibikenydata-2':            
        df = df.withColumn('start_date', to_timestamp(df['starttime'], 'MM/dd/yyyy HH:mm')).drop('starttime')\
               .withColumn('member_type', df['usertype'].cast(StringType())).drop('usertype')\
               .withColumn('ride_duration', df['tripduration'].cast(IntegerType())).drop('tripduration')\
               .withColumn('start_station', df['start station id'].cast(IntegerType())).drop('start station id')\
               .withColumn('end_station', df['end station id'].cast(IntegerType())).drop('end station id')\
               .withColumn('bike_number', df['bikeid'].cast(IntegerType())).drop('bikeid')

    if s3_bucket_name == 'divvy-bikeshare-data':            
        df = df.withColumn('start_date', to_timestamp(df['starttime'], 'MM/dd/yyyy HH:mm')).drop('starttime')\
               .withColumn('member_type', df['usertype'].cast(StringType())).drop('usertype')\
               .withColumn('ride_duration', df['tripduration'].cast(IntegerType())).drop('tripduration')\
               .withColumn('start_station', df['from_station_id'].cast(IntegerType())).drop('from_station_id')\
               .withColumn('end_station', df['to_station_id'].cast(IntegerType())).drop('to_station_id')\
               .withColumn('bike_number', df['bikeid'].cast(IntegerType())).drop('bikeid')

    if s3_bucket_name == 'divvy-bikeshare-data-1':            
        df = df.withColumnRenamed('start_time', 'start_date')\
               .withColumnRenamed('01 - Rental Details Local Start Time', 'start_date')\
               .withColumnRenamed('usertype', 'member_type')\
               .withColumnRenamed('User Type', 'member_type')\
               .withColumnRenamed('tripduration', 'ride_duration')\
               .withColumnRenamed('01 - Rental Details Duration In Seconds Uncapped', 'ride_duration')\
               .withColumnRenamed('from_station_id', 'start_station')\
               .withColumnRenamed('03 - Rental Start Station ID', 'start_station')\
               .withColumnRenamed('to_station_id', 'end_station')\
               .withColumnRenamed('02 - Rental End Station ID', 'end_station')\
               .withColumnRenamed('bikeid', 'bike_number')\
               .withColumnRenamed('01 - Rental Details Bike ID', 'bike_number')

    if s3_bucket_name == 'indegodata':            
        df = df.withColumnRenamed('start_time', 'start_date')\
               .withColumnRenamed('passholder_type', 'member_type')\
               .withColumnRenamed('duration', 'ride_duration')\
               .withColumnRenamed('bike_id', 'bike_number')    
    
    if s3_bucket_name == 'indegodata-1':
        df = df.withColumn('start_date', to_timestamp(df['start_time'], 'MM/dd/yyyy HH:mm')).drop('start_time')\
               .withColumn('member_type', df['passholder_type'].cast(StringType())).drop('passholder_type')\
               .withColumn('ride_duration', df['duration'].cast(IntegerType())).drop('duration')\
               .withColumn('start_station', df['start_station_id'].cast(IntegerType())).drop('start_station_id')\
               .withColumn('end_station', df['end_station_id'].cast(IntegerType())).drop('end_station_id')\
               .withColumn('bike_number', df['bike_id'].cast(IntegerType())).drop('bike_id')

    if s3_bucket_name == 'indegodata-2' or s3_bucket_name == 'indegodata-3':
        df = df.withColumn('start_date', to_timestamp(df['start_time'], 'MM/dd/yyyy HH:mm')).drop('start_time')\
               .withColumn('member_type', df['passholder_type'].cast(StringType())).drop('passholder_type')\
               .withColumn('ride_duration', df['duration'].cast(IntegerType())).drop('duration')\
               .withColumn('bike_number', df['bike_id'].cast(IntegerType())).drop('bike_id')

    if s3_bucket_name == 'metrobikes-bikeshare-data' or s3_bucket_name == 'metrobikes-bikeshare-data-2019':            
        df = df.withColumnRenamed('start_time', 'start_date')\
               .withColumnRenamed('passholder_type', 'member_type')\
               .withColumnRenamed('duration', 'ride_duration')\
               .withColumnRenamed('bike_id', 'bike_number')    

    if s3_bucket_name == 'metro-bikeshare-data-1':
        df = df.withColumn('start_date', to_timestamp(df['start_time'], 'MM/dd/yyyy HH:mm')).drop('start_time')\
               .withColumn('member_type', df['passholder_type'].cast(StringType())).drop('passholder_type')\
               .withColumn('ride_duration', df['duration'].cast(IntegerType())).drop('duration')\
               .withColumn('start_station', df['start_station_id'].cast(IntegerType())).drop('start_station_id')\
               .withColumn('end_station', df['end_station_id'].cast(IntegerType())).drop('end_station_id')\
               .withColumn('bike_number', df['bike_id'].cast(IntegerType())).drop('bike_id')
        
    return df
