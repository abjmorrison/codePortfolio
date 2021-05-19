########
# Date: 2021-02-25
# Contact: abjmorrison@gmail.com
# This script pulls all historical REACH JMMI data and writes a csv to s3

from bs4 import BeautifulSoup
import boto3
import os
import requests
import pandas as pd
import time
import random
import io
import datetime as dt
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
pages=[1,2,3]

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)
links=[]

for pg_num in pages:
    url="https://www.reach-initiative.org/where-we-work/countryName/page/"+str(pg_num)+"?pcountry=countryName&dates=Date&ptype=dataset-database&subpillar=cash"
    req = session.get(url)
    soup=BeautifulSoup(req.content, 'html.parser')
    for link in soup.findAll('a'):
        href=link.get('href')
        links.append(href)

links = [i for i in links if i]
links = [i for i in links if 'jmmi' in i.lower()]

#%%

objects=[]
for link in links:
    obj=pd.ExcelFile(link)
    sleep_time=random.uniform(0.1,15)
    time.sleep(sleep_time)
    objects.append((link,obj))
#%%
object_dict=dict(objects)
dataframe=pd.DataFrame()

#%%
for link, object in object_dict.items():
    date=link.split('_')[-1].split('.xlsx')[0]
    sheets=[x for x in object.sheet_names if ('governorate' in x.lower()) and ('medians' in x.lower())]
    print(sheet)
    for sheet in sheets:
        df=pd.read_excel(object, sheet_name=sheet)
        df['file']=link
        df['date']=date
        dataframe=pd.concat([df,dataframe], sort=True)

#%%
session=boto3.Session(aws_access_key_id="your_access_key_id", aws_secret_access_key="your_secret_access_key")
s3=session.client('s3')

in_bucket="bucketName"
in_key="objectKey"
csv_buffer=io.StringIO()
dataframe.to_csv(csv_buffer, index=False)
s3.put_object(Body=csv_buffer.getvalue(), Bucket=in_bucket, Key=in_key)
#%%
d = dt.date.today()
month = (d.replace(day=1) - dt.timedelta(days=1)).replace(day=d.day)
month = str(dt.datetime.strftime(month, '%b')).lower()
year = str(dt.datetime.today().year)

#%%
month
year
