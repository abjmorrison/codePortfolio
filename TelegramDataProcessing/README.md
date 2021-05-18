# Project: Telegram Data Processing
This project collects, processes, and consolidates daily messages from a Telegram channel.
It is implemented using AWS Lambda and orchestrated through EventBridge.

The pipeline consists of:
  1. [telegram-daily-apicall.py](https://github.com/abjmorrison/codePortfolio/blob/main/TelegramDataProcessing/telegram-daily-apicall.py)
      - Makes an API call to the Telegram API to get updates
  2. [Telegram_ETL.py](https://github.com/abjmorrison/codePortfolio/blob/main/TelegramDataProcessing/telegram-ETL.py)
      - Processes Telegram messages using AWS Translate and Pandas
  3. [telegram-ETL-crawlertrigger.py](https://github.com/abjmorrison/codePortfolio/blob/main/TelegramDataProcessing/telegram-ETL-crawlertrigger.py)
      - Triggers an AWS Glue crawler to consolidate daily data files
  4. [telegram-ETL-output.py](https://github.com/abjmorrison/codePortfolio/blob/main/TelegramDataProcessing/telegram-ETL-output.py)
      - Uses AWS Athena to output a consolidated CSV to use in visualizations
