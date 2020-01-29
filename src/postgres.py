import os
from pyspark.sql import DataFrameWriter


class SparkToPostgres(object):

    def __init__(self):

        self.pg_host = os.environ['PG_HOST']
        self.pg_database = 'bikeshare'
        self.pg_url = 'jdbc:postgresql://' + self.pg_host + ':5432/' + self.pg_database
        self.pg_properties = {
            'user': os.environ['PG_USER'],
            'password': os.environ['PG_PASS'],
            'driver': 'org.postgresql.Driver'
        }

        def write_to_postgres(self, df, tableName):

            df.write.jdbc(self.pg_url, table=tableName, mode="overwrite", self.pg_properties)
