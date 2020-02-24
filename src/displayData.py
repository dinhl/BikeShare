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


# Get data from Postgres DB to display on Dash app
df_rides_all_company_by_year, year_options = df_rides_all_company_by_year()
df_rides_by_company_all_year, company_options = df_rides_by_company_all_year()
df_all_member_type_all_year, company_options = df_all_member_type_all_year()


# Design the Dash app
app = dash.Dash()

app.layout = html.Div([
    html.Div([         
        # Display project name
        html.H1(
            children = 'Bike Share',
            style = {
                'textAlign': 'center',
                'color': '#009933',
                'padding': 0,
                'font-size': 80,      
                }
            ),                       
        ],),
        
        # Display graph 1 - Total numeber of rides in a selected year
        html.Div([
            # Drop down menu for selecting a year
            dcc.Dropdown(
                id = 'year-picker',
                options = year_options,
                placeholder = "Select a year",                
                style = {
                    'width': '200px', 'height': '50px', 'textAlign': 'left',
                    'font-size': 22, 'color': '#00004d','backgroundColor': '#997300',
                    'marginBottom': 8
                    }
                ),
            # Display the graph with details defined in callback function 1
            dcc.Graph(id = 'graph_rides_all_company_by_year'),
            ],
            style = {
                'width': '90%', 'float': 'center', 'padding-left': 80,
                'padding-bottom': 50, 'color': 'black'
                },
            ),
        
        # Display graph 2 - Total numeber of rides per year
        html.Div([
            # Drop down menu for selecting a company
            dcc.Dropdown(
                id = 'company-picker-left',
                options = company_options,\
                placeholder = 'Select a company',\
                style = {
                    'width': '400px', 'height': '50px', 'textAlign': 'left',
                    'font-size': 23, 'color': '#111111','backgroundColor': '#666666',
                    'marginBottom': 8, 'boder-color': 'red'
                    }
                ),
            # Display the graph with details defined in callback function 2
            dcc.Graph(id = 'graph_rides_by_company_all_year'), 
            ],
            style = {
                'width': '40%', 'display': 'inline-block', 'marginLeft': 80,
                'padding-bottom': 30, 'color': '#ffffff'
                },
            ),

        # Display graph 3 - Member type by year
        html.Div([
            # Drop down menu for selecting a company
            dcc.Dropdown(
                id = 'company-picker-right',
                options = company_options,
                placeholder = 'Select a company',                
                style = {
                    'width': '400px', 'height': '50px', 'textAlign': 'left',
                    'font-size': 23, 'color': '#111111','backgroundColor': '#666666',
                    'marginBottom': 8
                    }
                ),
            # Display the graph with details defined in callback function 3
            dcc.Graph(id = 'graph_all_member_type_all_year'),
            ],
            style = {
                'width': '40%', 'display': 'inline-block', 'float': 'right',
                'marginRight': 80, 'padding-bottom': 30, 'color': '#ffffff'
                },
            ),
    ],
    style = {'backgroundColor': '#111111', 'width': '100%', 'height': '100%'},)


@app.callback(Output('graph_rides_all_company_by_year', 'figure'),
             [Input('year-picker', 'value')])
def update_figure_selected_year(selected_year):
    """This is the callback function for graph 1

    Parameters:
        selected_year: a drop down menu for selecting a year

    Return:
        Display a graph with all the companies' business
        Each line represents a company
        xaxis -- months in a year
        yasix -- total number of rides
    """

    figure = go.Figure()
    
    filtered_df = df_rides_all_company_by_year[df_rides_all_company_by_year['ride_year'] == selected_year]
    
    traces = []

    for company_name in filtered_df['company_name'].unique():
        df_by_company = filtered_df[filtered_df['company_name'] == company_name]
        traces.append(go.Scatter(
            x = df_by_company['ride_month'],
            y = df_by_company['total_rides'],
            text = df_by_company['company_name'],
            mode = 'lines+markers',
            marker = {'size': 10},
            name = company_name
            ))

    return{
        'data': traces,
        'layout': go.Layout(
            plot_bgcolor = '#e6e6e6',
            paper_bgcolor = '#e6e6e6',            
            title = 'Total number of rides in a selected year',           
            xaxis = {'title': 'Month of the year', 'range': [0, 12]},
            yaxis = {'title': 'Total number of rides'},
            hovermode = 'closest',
            margin = {'l': 120, 'b': 120, 't': 120, 'r': 200},
            height = 700,            
            font = dict(size = 20, color = '#111111'),            
            ),        
        }


@app.callback(Output('graph_rides_by_company_all_year', 'figure'),
             [Input('company-picker-left', 'value')])
def update_figure_graph_left(selected_company):
    """This is the callback function for graph 2

    Parameters:
        selected_company: a drop down menu for selecting a company

    Return:
        Display a graph with a company's business over the years
        Each bar represents a year
        xaxis -- the years
        yasix -- total number of rides
    """

    filtered_df = df_rides_by_company_all_year[df_rides_by_company_all_year['company_name'] == selected_company]
    
    traces = []

    for company_name in filtered_df['company_name'].unique():
        df_by_company = filtered_df[filtered_df['company_name'] == company_name]
        traces.append(go.Bar(
            x = df_by_company['ride_year'],
            y = df_by_company['total_rides'],
            text = df_by_company['company_name'],
            type = 'bar',            
            name = company_name
            ))

    return {
        'data': traces,
        'layout': go.Layout(
            plot_bgcolor = '#b36b00',
            paper_bgcolor = '#b36b00',
            title = 'Total number of rides per years',
            xaxis = {'title': 'Year'},
            yaxis = {'title': 'Total number of rides'},
            hovermode = 'closest',
            margin = {'l': 120, 'b': 120, 't': 120, 'r': 120},
            height = 600,
            font = dict(size = 20, color = '#ffffff'),
            ),    
        }


@app.callback(Output('graph_all_member_type_all_year', 'figure'),
             [Input('company-picker-right', 'value')])
def update_figure_graph_right(selected_company):
    """This is the callback function for graph 3

    Parameters:
        selected_company: a drop down menu for selecting a company

    Return:
        Display a graph with the number of members vs casual users over the years
        Each bar represents a year
        xaxis -- the years
        yasix -- total number of rides
    """

    filtered_df = df_all_member_type_all_year[df_all_member_type_all_year['company_name'] == selected_company]
    
    traces = []

    trace1 = go.Bar(
        x = filtered_df['ride_year'], 
        y = filtered_df['total_casual'],
        name = 'Casual',
        marker = dict(color = '#7AC142')
        )

    trace2 = go.Bar(
        x = filtered_df['ride_year'], 
        y = filtered_df['total_member'],
        name = 'Member',
        marker = dict(color = '#2D5980')
        )

    return {
        'data': [trace2, trace1],
        'layout': go.Layout(
            plot_bgcolor = '#1f2e2e',
            paper_bgcolor = '#1f2e2e',
            title = 'Member type by year',
            xaxis = {'title': 'Year'},
            yaxis = {'title': 'Total number of rides'},
            hovermode = 'closest',
            margin = {'l': 120, 'b': 120, 't': 120, 'r': 120},
            barmode = 'stack',
            height = 600,
            font = dict(size = 20, color = '#ffffff'),            
            ),                
        }


if __name__ == '__main__':    
    app.run_server(host='0.0.0.0', port=8080)
