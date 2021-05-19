########
# Date: 2021-02-25
# Contact: abjmorrison@gmail.com
# This script queries Athena and outputs a consolidated csv to s3

import boto3
import time

client = boto3.client('athena')
result=client.update_work_group(WorkGroup='workgroupname')
s3 = boto3.resource("s3")

outURI="outputLocation"
uri_bucket="outputBucket"
uri_prefix="outputPrefix"
out_bucket="bucketName"
out_prefix="prefix"
out_key="objectKey"

def lambda_handler(event, context):

    try:
        #run query
        queryStart = client.start_query_execution(
            QueryString = 'SELECT * FROM  "database"."reach_jmmi" ',
            QueryExecutionContext = {
              'Database': "database"
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
            s3.Object(out_bucket, out_prefix+out_key).copy_from(CopySource=queryLoc[5:])
            s3.Object(uri_bucket,uri_prefix+queryId+'.csv').delete()
            s3.Object(uri_bucket,uri_prefix+queryId+'.csv.metadata').delete()
            print('CSV generated, query complete.')
        elif queryStatus=='FAILED':
            print('Query Failed. No CSV was written.')
        else:
            print('Query still running:', queryStatus)
    except Exception as e:
        print('Query did not succeed.')
        print(e)
    else:
        print('Crawler status:', crawler_status)
        print('Crawler did not succeed; could not trigger query.')


    return     {
        'queryStatus':queryStatus
    }
