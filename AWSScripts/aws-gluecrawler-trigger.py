# Set up logging
import json
import os
import logging
from time import sleep
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')

def trigger_glue_crawler(crawlerName, wait_until_done=True):

    try:
        crawler = client.get_crawler(Name=crawlerName)

        if crawler['Crawler']['State'] == "READY":
            crawler_response = client.start_crawler(Name = crawlerName)
            logger.info('## STARTED GLUE CRAWLER: ' + crawlerName)
            response = {"output":crawler_response,
                "crawlerName":crawlerName
            }
        else:
            response = {
                "output":None,
                "crawlerName":crawlerName
                }

        active_statuses=["RUNNING", "STOPPING","STOPPED"]
        if wait_until_done == True:
            while True:
                sleep(5)
                crawler = client.get_crawler(Name=crawlerName)
                crawler_status = crawler['Crawler']['State']
                if crawler_status in active_statuses:
                    sleep(5)
                    continue
                elif (crawler_status == "READY") & (response["output"]!=None):
                    lastCrawlStatus = json.loads(json.dumps(crawler, default=str))
                    lastCrawlStatus = lastCrawlStatus['Crawler']['LastCrawl']['Status']
                    response["status"] = "SUCCEEDED"
                    break
                else:
                    response["status"] = crawler_status
                    break
        else:
            print("Crawler started but not monitored.")
            return response

    except Exception as e:
        print(e)
        response = {"output":"crawler not started"}
        return response

def lambda_handler(event, context):
    crawlerName=event['crawlerName']
    crawler_response=trigger_glue_crawler(crawlerName)
    return crawler_response
