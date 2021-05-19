########
# Date: 2021-02-25
# Contact: abjmorrison@gmail.com
# This script pulls REACH JMMI data labeled for the present month and writes as csv to s3

from bs4 import BeautifulSoup
import io
import requests
import pandas as pd
import datetime as dt
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import boto3

url="https://www.reach-initiative.org/where-we-work/countryName/?pcountry=countryName&dates=Date&ptype=dataset-database&subpillar=cash"
s3=boto3.client('s3')
def lambda_handler():
    bucket="bucketName"
    key="objectKey"
    response = s3.head_object(Bucket=bucket, Key=key)
    datetime_value = 2 #pd.to_datetime(response["LastModified"]).month

    if datetime_value != dt.datetime.today().month:
        print('Data has not been updated this month')
        try:
            print('Getting HTML...')
            # set up session for retry handling
            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            links=[]
            # request html
            req = session.get(url)
            soup=BeautifulSoup(req.content, 'html.parser')
            # find all download links and append to list
            for link in soup.findAll('a'):
                href=link.get('href')
                links.append(href)

            # delete all None values
            links = [i for i in links if i]
            print('HTML parsed. Requesting data...')
            # get last month's dataset
            d = dt.date.today()
            month = (d.replace(day=1) - dt.timedelta(days=1)).replace(day=d.day)
            month = str(dt.datetime.strftime(month, '%B')).lower()
            year = str(dt.datetime.today().year)
            links = [i for i in links if (month in i.lower()) and (year in i.lower())]
            # get JMMI dataset
            links = [i for i in links if 'jmmi' in i.lower()]
            if len(links)>0:
                #create Excel objects
                objects=[]
                for link in links:
                    obj=pd.ExcelFile(link)
                    objects.append((link,obj))
                object_dict=dict(objects)
                dataframe=pd.DataFrame()
                print('Data pulled. Writing Data to dataframe...')
                #Send governorate medians sheets to dataframe
                for link, object in object_dict.items():
                    date=link.split('_')[-1].split('.xlsx')[0]
                    sheets=[x for x in object.sheet_names if ('governorate' in x.lower()) and ('medians' in x.lower())]
                    for sheet in sheets:
                        df=pd.read_excel(object, sheet_name=sheet)
                        df['file']=link
                        df['month']=str(datetime_value)
                        df['year']=str(dt.datetime.today().year)
                        df['date']=df.year+'-'+df.month+'-'+'01'
                        df['date']=pd.to_datetime(df.date)
                        dataframe=pd.concat([df,dataframe], sort=True)

                # write csv to s3
                in_bucket="bucketName"
                in_key="objPrefix"+month+'_'+year+'objKey'
                csv_buffer=io.StringIO()
                dataframe.to_csv(csv_buffer, index=False)
                s3.put_object(Body=csv_buffer.getvalue(), Bucket=in_bucket, Key=in_key)
                print('Dataframe written to S3')

                return {"output": "True"}

            else:
                print('No datasets have been published this month.')
                return {"output": "False"}

        except Exception as e:
            print('Exception occurred:', e)

    else:
        print("Dataset already updated this month.")
        return {"output":"False"}
#%%
lambda_handler()
