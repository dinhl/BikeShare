import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from dash.dependencies import Input, Output
from sqlalchemy import create_engine

from postgresQuery import df_rides_all_company_by_year  # Custom, see postgresQuery.py
from postgresQuery import df_rides_by_company_all_year  # Custom, see postgresQuery.py
from postgresQuery import df_all_member_type_all_year   # Custom, see postgresQuery.py


df_rides_all_company_by_year, year_options = df_rides_all_company_by_year()
df_rides_by_company_all_year, company_options = df_rides_by_company_all_year()
df_all_member_type_all_year, company_options = df_all_member_type_all_year()

colors = {
    'background': '#111111',
    'text': '#009933'
}

app = dash.Dash()

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Bike Share',
        style={
            'textAlign': 'center',
            'color': colors['text'],      
        }
    ),
    
    dcc.Dropdown(id='year-picker',options=year_options,value=df_rides_all_company_by_year['ride_year'].min()),
    dcc.Graph(id='graph_rides_all_company_by_year'),
    
    html.Hr(),

    dcc.Dropdown(id='company-picker-left', options=company_options,\
    	value=df_rides_by_company_all_year['company_name']),
    
    dcc.Graph(id='graph_rides_by_company_all_year'),

    html.Hr(),    
    
    dcc.Dropdown(id='company-picker-right', options=company_options, \
    	value=df_all_member_type_all_year['company_name']),
    
    dcc.Graph(id='graph_all_member_type_all_year')        
    ],
    )


@app.callback(Output('graph_rides_all_company_by_year', 'figure'),
              [Input('year-picker', 'value')])
def update_figure_selected_year(selected_year):
   
    figure=go.Figure()
    filtered_df = df_rides_all_company_by_year[df_rides_all_company_by_year['ride_year'] == selected_year]
    traces = []

    for company_name in filtered_df['company_name'].unique():
        df_by_company = filtered_df[filtered_df['company_name'] == company_name]
        traces.append(go.Scatter(
            x=df_by_company['ride_month'],
            y=df_by_company['total_rides'],
            text=df_by_company['company_name'],
            mode='lines+markers',
            opacity=0.7,
            marker={'size': 20},
            name=company_name
        ))

    return {
        'data': traces,
        'layout': go.Layout(
            plot_bgcolor='#111111',
            paper_bgcolor='#111111',
            title={'text': 'Total number of rides in a selected year'},
	        xaxis={'title': 'Month of the year'},
            yaxis={'title': 'Total number of rides'},
            hovermode='closest',
            font=dict(
                family='Courier New, monospace',
                size=16,
                color='#7f7f7f'),            
            ),
    }


@app.callback(Output('graph_all_member_type_all_year', 'figure'),
              [Input('company-picker-right', 'value')])
def update_figure(selected_company):
    filtered_df = df_all_member_type_all_year[df_all_member_type_all_year['company_name'] == selected_company]
    traces = []

    trace1 = go.Bar(
        x=filtered_df['ride_year'], 
        y=filtered_df['total_casual'],
        name = 'Casual',
        marker=dict(color='#7AC142')
    )

    trace2 = go.Bar(
        x=filtered_df['ride_year'], 
        y=filtered_df['total_member'],
        name = 'Member',
        marker=dict(color='#2D5980')
    )

    return {        
        'data': [trace2, trace1],
        'layout': go.Layout(
            plot_bgcolor='#111111',
            paper_bgcolor='#111111',
            title='Member type by year',
            xaxis={'title': 'Year'},
            yaxis={'title': 'Total number of rides'},
            hovermode='closest',
            barmode='stack'        
        )
    }


@app.callback(Output('graph_rides_by_company_all_year', 'figure'),
              [Input('company-picker-left', 'value')])
def update_figure(selected_company):
    filtered_df = df_rides_by_company_all_year[df_rides_by_company_all_year['company_name'] == selected_company]
    traces = []
    for company_name in filtered_df['company_name'].unique():
        df_by_company = filtered_df[filtered_df['company_name'] == company_name]
        traces.append(go.Bar(
            x=df_by_company['ride_year'],
            y=df_by_company['total_rides'],
            text=df_by_company['company_name'],
            type='bar',            
            name=company_name
        ))

    return {
        'data': traces,
        'layout': go.Layout(
            title='Total number of rides per years',
            xaxis={'title': 'Year'},
            yaxis={'title': 'Total number of rides'},
            hovermode='closest'        
        )        
    }

if __name__ == '__main__':
    app.run_server()