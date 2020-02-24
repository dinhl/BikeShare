import os
from pyspark.sql import DataFrameWriter


def write_to_postgres(df, table_name):
    """
    Write PySpark dataframes to Postgres Database

    Parameters:
    	df -- the dataframe with extracted data
    	table_name -- the table name in Postgres Database
    """

    pg_host = os.environ['PG_HOST']
    pg_database = 'bikeshare'
    pg_url = 'jdbc:postgresql://' + pg_host + ':5432/' + pg_database
    pg_properties = {
        'user': os.environ['PG_USER'],
        'password': os.environ['PG_PASS'],
        'driver': 'org.postgresql.Driver'
    }

    df.write.jdbc(url = pg_url, table = table_name, mode = "append", properties = pg_properties)
