
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday 16 February 2021

This script performs an ad hoc query in AWS Athena to output a .csv of consolidated telegram messages.

Version 1.0
"""

import boto3
import time

client = boto3.client('athena')
result=client.update_work_group(WorkGroup='workgroup1')
s3 = boto3.resource("s3")

outURI="s3://URIPATH"
out_bucket="BUCKETNAME"
out_prefix="PREFIX"
out_key="FILENAME"

def lambda_handler(event, context):
    crawler_status=event['detail']['state']
    if crawler_status=='Succeeded':
        try:
            #run query
            queryStart = client.start_query_execution(
                QueryString = 'SELECT * FROM  "DATABSE"."TABLE" ',
                QueryExecutionContext = {
                  'Database': "DATABASE"
                },
                ResultConfiguration = {
                  #query result output location you mentioned in AWS Athena
                  "OutputLocation": outURI }
              )

            #executes query and waits
            queryId = queryStart['QueryExecutionId']
            time.sleep(15)
            # get query response object
            response = client.get_query_execution(
                QueryExecutionId=queryId
            )
            queryStatus=response['QueryExecution']['Status']['State']
            queryStates=['SUBMITTED','QUEUED', 'RUNNING','SUCCEEDED','FAILED']

            if queryStatus=='SUCCEEDED':
                print('Query succeeded! Renaming files...')
                #create copy of CSV with proper filename and delete Athena outputs
                queryLoc=response['QueryExecution']['ResultConfiguration']['OutputLocation']
                queryId=response['QueryExecution']['QueryExecutionId']
                s3.Object(out_bucket, out_prefix+'FILENAME.csv').copy_from(CopySource=queryLoc[5:])
                s3.Object(out_bucket,out_prefix+queryId+'.csv').delete()
                s3.Object(out_bucket,out_prefix+queryId+'.csv.metadata').delete()
                print('CSV generated, query complete.')
            elif queryStatus=='FAILED':
                print('Query Failed. No CSV was written.')
            else:
                print('Query still running:', queryStatus)

        except Exception as e:
            print('Query did not succeed.')
            print(e)
            raise(e)
    else:
        print('Crawler status:', crawler_status)
        print('Crawler did not succeed; could not trigger query.')
