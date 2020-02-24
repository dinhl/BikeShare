import os
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import types
from postgres import write_to_postgres      # Custom, see postgres.py
from dataCleaning import rename_column      # Custom, see dataCleaning.py
from dataCleaning import get_s3_bucket_list # Custom, see dataCleaning.py
from dataCleaning import get_company_name   # Custom, see dataCleaning.py
from dataCleaning import get_column_name    # Custom, see dataCleaning.py


def main():
    """
    Main ETL for aggregating data
        Handled Spark session set up
        Read data from AWS S3
        Renamed selected columns
        Extracted, Transformed data
        Loaded data to Postgres Database
    """

    spark = SparkSession.builder.appName('BikeShare').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print(chr(27) + '[2J')

    s3_prefix = 's3a://'
    s3_file_name = '/*.csv'    
    s3_buckets_list = get_s3_bucket_list()
    company_name = get_company_name()
    column_name = get_column_name()  

    for s3_bucket_name in s3_buckets_list:        
        s3_url = s3_prefix + s3_bucket_name + s3_file_name

        df = spark.read.csv(s3_url, header = True, inferSchema = True, 
            timestampFormat = 'MM/dd/yyyy HH:mm')
        
        df = rename_column(df, s3_bucket_name)        
        
        df.createOrReplaceTempView("sql_view")
        
        processed_data = spark.sql(\
            "SELECT '{0}' as company_name,\
                date_trunc('hour', `{1}`) as ride_date,\
                (dayofweek(date_trunc('day', `{1}`))) as ride_dow,\
                case when `{2}` = 'Subscriber' then 'Member' \
                when `{2}` = 'Dependent' then 'Member' \
                when `{2}` = 'Indego365' then 'Member' \
                when `{2}` = 'Indego30' then 'Member' \
                when `{2}` = 'IndegoFlex' then 'Member' \
                when `{2}` = 'MonthlyPass' then 'Member' \
                when `{2}` = 'Monthly Pass' then 'Member' \
                when `{2}` = 'Annual Pass' then 'Member' \
                when `{2}` = 'Flex Pass' then 'Member' \
                when `{2}` = 'Customer' then 'Casual'\
                when `{2}` = 'Daily' then 'Casual'\
                when `{2}` = 'Day Pass' then 'Casual'\
                when `{2}` = 'Walk-up' then 'Casual'\
                when `{2}` = 'One Day Pass' then 'Casual'\
                when `{2}` = 'Two Day Pass' then 'Casual'\
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
                `{2}`".format(\
                company_name[s3_bucket_name],
                column_name[0],
                column_name[1],
                column_name[2],
                column_name[3],
                column_name[4],
                column_name[5]
            ))
        
        write_to_postgres(processed_data, "bikeshare_data")

    spark.stop()

if __name__ == "__main__":
    main()
