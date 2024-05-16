import requests
import pandas as pd
import yaml
from to_sql import data_to_sql
import argparse
import datetime
import logging
import azure.functions as func

path_to_file = "../lib/cc.yaml"

def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%d/%m/%Y")

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--startfrom', type=parse_date, default=datetime.datetime.now() - datetime.timedelta(days=2), help= "where do you want to start from")
parser.add_argument('-e', '--endto', type=parse_date, default=datetime.datetime.now(), help= "where do you want to end to")

args = parser.parse_args()

with open(path_to_file, "r") as file:
    config = yaml.safe_load(file)

# Get the api key and the stocks to bulk import
api_key = config["Api"]["Alphavantage"]
stocks = config["Companies_bulk"]["Names"]


def get_historic_data(api_key, stocks, args):
    all_rows = []
    for company_names, stock in stocks.items(): #Iterate over each company
        req = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize=full'.format(stock, api_key)) #Api connection url
        data_json = req.json()
        daily_data = data_json["Time Series (Daily)"]
        
        for dates, info in daily_data.items(): # Access daily trading stats, append the rows, convert all the rows into dataframe
            day_row = { "company": company_names,
                       "stock": stock,
                        "trading_day" : dates,
                        "open_price" : info['1. open'],
                        "high_price" : info['2. high'],
                        "low_price": info['3. low'],
                        "close_price": info['4. close'],
                        "volume": info['5. volume']
                    }
            all_rows.append(day_row)
    df = pd.DataFrame(all_rows)

    # Convert 'trading_day' to datetime for accurate filtering and comparison
    df['trading_day'] = pd.to_datetime(df['trading_day'])

    if args.startfrom:
        df = df[(df['trading_day'] >= args.startfrom) & (df['trading_day'] <= args.endto)]

    # Change the datatype of the columns and ensure the change is applied
    df = df.astype({"company": 'category', "stock": 'category', 'open_price': 'float', 'high_price': 'float', 'low_price': 'float', 'close_price': 'float', 'volume': 'int'})
    
    # Import to the database
    data_to_sql(df)
    
get_historic_data(api_key, stocks, args)





