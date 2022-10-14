import os
from google.cloud import storage

import yfinance as yf
import pandas as pd
import string

"""Get runtime environment variables"""
project_id = os.environ.get('project_id')
bucket_name = os.environ.get('bucket_name')
storage_client = storage.Client(project='project_id')

def upload_blob(bucket_name, source_data, destination_blob_name):
    """Uploads a file to the bucket."""    
    print('function upload_blob called')     
    bucket = storage_client.get_bucket(bucket_name)    
    blob = bucket.blob(destination_blob_name)    
    blob.upload_from_string(source_data)    
    print('File {} uploaded to {}.'.format(destination_blob_name, bucket_name))

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket"""
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        print(blob.name)

def get_clean_upload_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    """Scrap all Thai tickers name form set.or.th"""     
    # Create list A-Z
    alphabet = list(string.ascii_uppercase)
    print(alphabet, end='')

    # URL for ticker prefix A-Z
    url_list = ['https://classic.set.or.th/set/commonslookup.do?language=en&country=US&prefix=' + i for i in alphabet]

    # URL for ticker prefix 0-9
    url_number_prefix = 'https://classic.set.or.th/set/commonslookup.do?language=en&country=US&prefix=NUMBER'

    # Create dataframe and scrap history data
    # Start with prefix 0-9
    data_table = pd.read_html(url_number_prefix)

    # Prefix A-Z
    for i in url_list:
      progress_table = pd.read_html(i)
      data_table[0] = data_table[0].append(progress_table[0], ignore_index=True)

    # Create list of thai tickers name
    tickers = data_table[0]['Symbol'].tolist()

    # Thai tickers's name end with '.BK'
    thai_tickers = [i + '.BK' for i in tickers]

    # Convert thai_tickers into object used by yfinance module
    tickers = [yf.Ticker(ticker) for ticker in thai_tickers]

    # Get price and dividends data
    data_hist = [] # create list to store all table
    for ticker in tickers:
      hist = ticker.history(start='2021-01-01') # ดึงข้อมูลพื้นฐานของหุ้นแต่ละตัว เริ่มที่ปี 2021

      # Create column 'Date' for index
      hist = hist.reset_index()
      hist.columns = ['Date', *hist.columns[1:]]

      # Create column  'Ticker' for ticker name
      hist['Ticker'] = ticker.ticker

      data_hist.append(hist)

    # Convert all table in list to dataframe
    parser = pd.io.parsers.base_parser.ParserBase({'usecols' : None})

    for data in data_hist:
      data.columns = parser._maybe_dedup_names(data.columns)

    data = pd.concat(data_hist, ignore_index=True)
    data = data.set_index(['Ticker', 'Date'])

    """Clean data"""

    clean_data = data

    # Drop columns that not needed
    clean_data = clean_data.drop(['Open', 'High', 'Low', 'Volume', 'Stock Splits', 'Adj Close'], axis=1)

    # Rename column 'Close' to 'Price'
    clean_data.rename(columns = {'Close':'Price'}, inplace = True)

    # Erase .BK from column 'Ticker'
    # Need to reset_index before rename index column
    clean_data = clean_data.reset_index()
    clean_data['Ticker'] = clean_data.apply(lambda x: x['Ticker'].replace(".BK",""), axis=1)

    # set_index
    clean_data = clean_data.set_index(['Ticker', 'Date'])

    """Upload Data"""
    local_data = clean_data.to_csv()
    file_name = 'price_and_dividend.csv'
    upload_blob(bucket_name, local_data, file_name)
    print('Data inside of',bucket_name,':')
    return list_blobs(bucket_name)
