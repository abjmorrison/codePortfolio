########
# Date: 2021-02-11
# This script processes messages from Telegram in Arabic
# It is configured to run on AWS Lambda

import boto3
import numpy as np
import pandas as pd
import re
from io import StringIO
from time import sleep
import random
from datetime import datetime, timedelta
import urllib
import json

# Location of inputs / outputs
yesterday=datetime.today() - timedelta(days=1)
out_bucket='BucketName'
out_key='outputKeyString'+str(yesterday.date())

# Start client for AWS Services
s3=boto3.client('s3')

def translate_text(text):
    '''
    this function translates a text string from arabic to english.
    change SourceLangageCode and TargetLanguageCode to translate between any two supported languages
    '''
    translate = boto3.client(service_name='translate', use_ssl=True)
    try:
        if len(text)>0 and len(text.encode('utf-8'))<5000:
            result = translate.translate_text(Text=text,
                                              SourceLanguageCode="ar", TargetLanguageCode="en")['TranslatedText']
            result=result.lower()
            result=result.encode('ascii', 'ignore').decode('ascii')
            randomsleep=random.uniform(0.001, 3.001)
            sleep(randomsleep)
            return result
        elif len(text.encode('utf-8'))>= 5000:
            print('Translate could not translate this text because it was longer than 5000 bytes!')
        else: pass
    except Exception as e:
        print(e)

def drop_empty_str(x):
    '''this function drops empty strings from a list'''
    if x is not None:
        return x
    else:
        pass

def process_messages(in_bucket, in_key):
    #get json from s3
    jsonObj=s3.get_object(Bucket=in_bucket, Key=in_key)['Body'].read()
    df=pd.read_json(jsonObj)
    if len(df)>0:
        df.columns=['date','message']
        df.date=pd.to_datetime(df.date)

        #translate text to english
        df['english']=df.message.apply(translate_text)
        #split english message into city1 and city2 information
        df[['nocol','city1','city2']]=df.english.str.split('currency', expand=True)
        df.dropna(subset=['english'], inplace=True)
        #split city1 and city2 into a list of strings containing buy/sell/currency/value for the date
        for city in ['city1','city2']:
            df[city]=df[city].str.replace(' ','').str.split('\n', expand=False)
            df[city]=df[city].apply(drop_empty_str)
        #explode by column list
        a=df.explode('city1')[['date','city1']]
        s=df.explode('city2')[['date','city2']]
        cities={'city1':a, 'city2':s}

        #extract exchange rate information for each city and concat into final dataframe
        final=[]
        for city in cities.keys():
            ck=cities[city]
            ck['region']=city
            ck['currency'] = np.where(ck[city].str.contains(""), "usd",
                           np.where(ck[city].str.contains("currency2"), "currency2", np.nan))
            ck['buy_sell'] = np.where(ck[city].str.contains("buy"), "buy",
                           np.where(ck[city].str.contains("sell") | ck[city].str.contains("sale"), "sell", np.nan))
            ck['value'] = [re.findall('[\d\.\d]+', str(x)) for x in ck[city] ]
            ck=ck.explode('value')
            ck=ck[['date','region','buy_sell','currency','value']]
            ck=ck.loc[ck.currency!='nan']
            final.append(ck)

        #needs to write lines to csv
        final=pd.concat(final)
        final['date']=final.date.astype(str).str.split(' ')
        final['date']=[x[0] for x in final.date]
        #output dataframe to csv format and save to s3
        csv_buffer=StringIO()
        final.to_csv(csv_buffer, index=False)
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=out_bucket, Key=out_key+'.csv')

        #output df to json format - uncomment if .json is desired output
        #final=list(final.to_dict('records'))
        #msgBytes=str(json.dumps(final).encode('UTF-8'))
        #remove list brackets to avoid problems reading into data catalog
        #msgBytes.replace('[','').replace(']','')
        #s3.put_object(
             #Body=msgBytes,
             #Bucket=out_bucket,
             #Key=out_key+'.json')

        print('New Telegram messages written to s3:', out_bucket+'/'+out_key)
    else:
        print('no messages yesterday!')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    in_bucket = event['Records'][0]['s3']['bucket']['name']
    in_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        process_messages(in_bucket,in_key)
    except Exception as e:
        print(e)
        raise e
