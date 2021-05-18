
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday 16 February 2021

This script initiates a crawler in AWS Glue in order to create a consolidated datatable.
The crawler must be pre-created in AWS Glue

Version 1.0
"""

import json
import os
import logging

# set logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# start boto3 client for glue
import boto3
client = boto3.client('glue')

# name of crawler (must already be created)
crawlerName = "Name of Crawler"

def lambda_handler(event, context):
    try:
        # initiate crawler; crawler will return error if already started/running
        response = client.start_crawler(Name = crawlerName)
        logger.info('## STARTED GLUE CRAWLER: ' + crawlerName)
    except Exception as e:
        print(e)
    return response
