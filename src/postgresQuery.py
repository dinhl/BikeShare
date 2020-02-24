import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from dash.dependencies import Input, Output
from sqlalchemy import create_engine


def connect_postgres():
    """Connected Dash app to Postgres Database"""

    pg_user = os.environ['PG_USER']
    pg_host = os.environ['PG_HOST']
    pg_pass = os.environ['PG_PASS']
    pg_port = 5432
    pg_database = 'bikeshare'

    pg_url = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'\
        .format(pg_user, pg_pass, pg_host, pg_port, pg_database)

    engine = create_engine(pg_url)
    return engine
    
engine = connect_postgres()
connection = engine.connect()


def postgres_query():
    """Generated queries to get data from Postgres Database"""

    query_rides_all_company_by_year = "SELECT company_name,\
    	extract(year FROM ride_date) as ride_year,\
    	extract(month FROM ride_date) as ride_month,\
    	sum(total_rides) as total_rides\
    	FROM bikeshare_data\
    	GROUP BY company_name, extract(year from ride_date), extract(month from ride_date)\
    	ORDER BY company_name, extract(year from ride_date), extract(month from ride_date)"

    query_rides_by_company_all_year = "SELECT company_name,\
        extract(year FROM ride_date) as ride_year,\
        sum(total_rides) as total_rides FROM bikeshare_data\
        GROUP BY company_name, extract(year FROM ride_date)\
        ORDER BY company_name, extract(year FROM ride_date)"

    query_all_member_type_all_year = "SELECT company_name,\
        extract(year FROM ride_date) as ride_year,\
        extract(month FROM ride_date) as ride_month,\
        sum(total_rides) as total_rides,\
        sum(case when member='Casual' then total_rides else 0 end) as total_casual,\
        sum(case when member='Member' then total_rides else 0 end) as total_member\
        FROM bikeshare_data\
        GROUP BY company_name, extract(year from ride_date), extract(month from ride_date)\
        ORDER BY company_name, extract(year from ride_date), extract(month from ride_date)"
       
    query_dow = "SELECT company_name,\
    	to_char(ride_date, 'yyyy-mm') as ride_month,\
    	ride_dow,\
    	sum(case when member='Casual' then total_rides else 0 end) as total_casual,\
    	sum(case when member='Member' then total_rides else 0 end) as total_member\
    	from bikeshare_data\
    	group by company_name, to_char(ride_date, 'yyyy-mm'), ride_dow"

    query_duration = "SELECT company_name,\
    	to_char(ride_date, 'yyyy-mm') as ride_month,\
    	ride_dow,\
    	sum(total_rides_under_30min) as short_trip,\
    	sum(total_rides_greater_30min) as long_trip\
    	from bikeshare_data\
    	group by company_name, to_char(ride_date, 'yyyy-mm'), ride_dow"
    
    query_list = {
        'query_rides_all_company_by_year': query_rides_all_company_by_year,
    	'query_all_member_type_all_year': query_all_member_type_all_year,
        'query_rides_by_company_all_year': query_rides_by_company_all_year
        }
    
    return query_list


def df_rides_all_company_by_year():
    """Return data with all companies' business in a selected year"""

    query_list = postgres_query()
    query_result = query_list['query_rides_all_company_by_year']
    df = pd.read_sql_query(query_result, engine)    

    df['ride_year'] = df['ride_year'].fillna(0).astype(np.int64)
    df['ride_month'] = df['ride_month'].fillna(0).astype(np.int64)

    year_options = []

    for year in df['ride_year'].unique():
        year_options.append({'label':str(year),'value':year})
    
    return df, year_options


def df_rides_by_company_all_year():
    """Return data with a company's business over the years"""

    query_list = postgres_query()
    query_result = query_list['query_rides_by_company_all_year']
    df = pd.read_sql_query(query_result, engine)
    
    df['ride_year'] = df['ride_year'].fillna(0).astype(np.int64)
    df['total_rides'] = df['total_rides'].fillna(0).astype(np.int64)

    company_options = []
    
    for company in df['company_name'].unique():
        company_options.append({'label':str(company),'value':str(company)})

    return df, company_options


def df_all_member_type_all_year():
    """Return data with the number of members vs casual users over the years"""

    query_list = postgres_query()
    query_result = query_list['query_all_member_type_all_year']
    df = pd.read_sql_query(query_result, engine)

    df['ride_year'] = df['ride_year'].fillna(0).astype(np.int64)
    df['total_rides'] = df['total_rides'].fillna(0).astype(np.int64)
    df['total_member'] = df['total_member'].fillna(0).astype(np.int64)
    df['total_casual'] = df['total_casual'].fillna(0).astype(np.int64)

    company_options = []

    for company in df['company_name'].unique():
        company_options.append({'label':str(company),'value':str(company)})

    return df, company_options
