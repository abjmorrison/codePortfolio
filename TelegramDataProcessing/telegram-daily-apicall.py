########
# Date: 2021-02-11
# This script queries the Telegram API for messages on a daily basis for a specified channel

import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
import io

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
import boto3
import nest_asyncio

nest_asyncio.apply()

api_id = os.environ['api_id']
api_hash = os.environ['api_hash']
phone = os.environ['phone']
username = os.environ['username']
auth_key = os.environ['auth_key']

def lambda_handler(event, context):

    chat = "https://t.me/CHATNAME" # Telegram chat to get messages from
    client = TelegramClient(StringSession(auth_key), api_id, api_hash)
    yesterday = datetime.today() - timedelta(days=1)
    messages = []
    os.chdir("/tmp") #change to tmp dir to store session files

    try:
        async def main(phone):
            await client.start()
            print("Client Created")
            if await client.is_user_authorized() == False:
                await client.send_code_request(phone)
                print('user was not authorized')
                try:
                    await client.sign_in(phone, input('Enter the code: '))
                except SessionPasswordNeededError:
                    await client.sign_in(password=input('Password: '))
            # get messages since yesterday
            async with client:
                async for msg in client.iter_messages(chat, offset_date=yesterday, reverse=True):
                    date=str(msg.date)
                    messages.append((msg.date, msg.text))

            if len(messages)<=0:
                print('there were no telegram messages in the last 24 hours')
            else:
                preBucket='preBucketName'
                preKey='preKeyString'
                # Save the new messages in JSON format
                # overwrites yesterday's data pull!
                msgBytes=json.dumps(messages, default=str).encode('UTF-8')
                s3=boto3.client('s3')
                s3.put_object(
                     Body=msgBytes,
                     Bucket=preBucket,
                     Key=preKey+'.json')
                print('new messages saved to ',preBucket+'/'+preKey)

        with client:
            client.loop.run_until_complete(main(phone))

    except Exception as e:
        print('You got no data from Telegram.')
        print(e)
