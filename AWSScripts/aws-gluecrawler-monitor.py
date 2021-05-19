########
# Date: 2021-03-01
# Contact: abjmorrison@gmail.com
# This script polls a crawler by crawler name to get crawler status

import boto3

client=boto3.client('glue')

def poll_crawler_status(crawlerName):
    crawlerStatus = client.get_crawler(Name=crawlerName)
    crawlerStatus = crawlerStatus["Crawler"]["State"]

    response = {"crawlerStatus":crawlerStatus}

    return response

def lambda_handler(event, context):
    crawlerName = event['crawlerName']
    crawler_status = poll_crawler_status(crawlerName)
    return crawler_status
