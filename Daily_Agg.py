import requests
import datetime
import os
from dotenv import load_dotenv
import pandas as pd

# Load env variables form .env fiel
load_dotenv()

# Change csv with stock tickers into Dataframe
tickers = pd.read_csv('stocks_list.csv')
daily_agg = pd.DataFrame(columns=['Ticker', 'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Volume Weighted Average Price', 'Total Transaction', 'Adjusted'])

# Initialize required Variables
api_key = os.getenv('Poly_API_Key')
mult = 1
timeframe = 'day'
start_time = '2024-04-30'
end_time = '2024-05-01'
limit = 50000

def json_df(ticker, json, df):
    row = {
        'Ticker' : ticker,
        'Open Time' : datetime.datetime.fromtimestamp(json['t']/1000),
        'Open' : json['o'],
        'High' : json['h'],
        'Low' : json['l'],
        'Close' : json['c'],
        'Volume' : json['v'],
        'Volume Weighted Average Price' : json['vw'],
        'Total Transaction' : json['n'],
        'Adjusted' : 'True'
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return df

def pull_agg(ticker, mult, timeframe, start, end, limit, api):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{mult}/{timeframe}/{start}/{end}?adjusted=true&sort=asc&limit={limit}&apiKey={api}'
    response = requests.get(url).json()
    return response

for ticker in tickers['Symbol']:
    i = 0
    daily_agg = pd.DataFrame(columns=['Ticker', 'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Volume Weighted Average Price', 'Total Transaction', 'Adjusted'])
    response = pull_agg(ticker, mult, timeframe, start_time, end_time, limit, api_key)
    for response in response['results']:
        daily_agg = json_df(ticker, response, daily_agg)
        i += 1
        print(i)
    daily_agg.to_csv(f'{ticker}_Daily_Agg.csv', index=False)
        