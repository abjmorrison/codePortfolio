#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 18:03:48 2020
@author: abjmorrison@gmail.com
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import boto3
import io
#%%
session=boto3.Session(aws_access_key_id='ACCESSKEYID',aws_secret_access_key='SECRETACCESSKEY')
s3=session.client('s3')
bucket_name='bucketName'
object_key='objectKey'
csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')
df = pd.read_csv(io.StringIO(csv_string), parse_dates=True)
##### Local File pull #####
#df=pd.read_csv('data_file.csv')
#####
df.date=pd.to_datetime(df.date)
df=df.sort_values(by=['date'])
external_stylesheets=["style.css"]
# Initialise the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
# Define the app
def get_options(list_zocs):
    dict_list = []
    for i in list_zocs:
        dict_list.append({'label': i, 'value': i})
    return dict_list

dropdown1=html.Div(className='div-for-dropdown',
          children=[
              dcc.Dropdown(id='zocselector',
                           options=get_options(df['zoc'].unique()),
                           multi=True,
                           value=[df['zoc'].sort_values()[0]],
                           className='zocselector')
                    ])
dropdown2=html.Div(className='div-for-dropdown',
          children=[
              dcc.Dropdown(id='itemselector',
                           options=get_options(df['item_id'].unique()),
                           className='itemselector')
                    ])

graph1=dcc.Graph(id='timeseries', config={'displayModeBar': False})

right_children=[dropdown1, dropdown2, graph1]

left_children = [
    html.H2('National Food price predictions with observed price points'),
    html.P('''Visualizing time series food prices'''),
    html.P('''Pick one or more ZOCs from the dropdown below to view historical time series with predicted prices
           from 12-2020 thru 03-2021''')
]
app.layout = html.Div(children=[
                      html.Div(className='row',  # Define the row element
                               children=html.Div(className='row',  # Define the row element
                                   children=[
                                      html.Div(className='four columns div-user-controls', children=left_children),  # Define the left element
                                      html.Div(className='eight columns div-for-charts bg-grey', children=right_children),  # Define the right element
                                      ]))
                                    ])

@app.callback(Output('timeseries', 'figure'),
              [Input('zocselector', 'value'),
              Input('itemselector','value')]
              )

def update_timeseries(selected_zoc, selected_item):
    ''' Draw traces of the feature 'value' based one the currently selected stocks '''
    # STEP 1
    trace = []
    df_sub = df.sort_values(by=['zoc','item_id','date'])
    # STEP 2
    # Draw and append traces for each stock
    for zoc in selected_zoc:
        graph_obj=go.Scatter(x=df_sub[(df_sub['zoc']==zoc)&(df_sub['item_id']==selected_item)]['date'],
                            y=df_sub[(df_sub['zoc']==zoc)&(df_sub['item_id']==selected_item)]['p50'],
                             mode='lines+markers',
                             opacity=0.7,
                             name=zoc,
                             textposition='bottom center')
        trace.append(graph_obj)

    # STEP 3
    traces = [trace]
    data = [val for sublist in traces for val in sublist]
    # Define Figure
    # STEP 4
    figure = {'data': data,
              'layout': go.Layout(
                  hovermode='x',
                  title={'text': 'Food Price Predictions: 12-2020 thru 03-2021', 'font': {'color': 'white'}},
                  xaxis={'type':'date',
                  'title_text':'Date'
                  },
                  yaxis={'title_text':'Currency'}
              )
              }

    return figure
# Run the app
# If running on local machine, remove host parameter.
if __name__ == '__main__':
    app.run_server(host='0.0.0.0',port=8080, debug=True)


#%%
