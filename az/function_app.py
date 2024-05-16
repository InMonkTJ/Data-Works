import logging
import azure.functions as func
import requests
import pandas as pd
import yaml
from dd import data_to_sql
import datetime
import os

# Determine the path to the cc.yaml file
path_to_file = os.path.join(os.path.dirname(__file__), '..', 'shared', 'cc.yaml')

def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%d/%m/%Y")

# Set default values for start and end dates
start_date = datetime.datetime.now() - datetime.timedelta(days=2)
end_date = datetime.datetime.now()

api_key = 'GNRD8ASXGY3VBTJY'
stocks = {
          "Chevron Corporation" : "CVX",
          "ExxonMobil" : "XOM",
          "Shell plc" : "SHEL"
          }

def get_historic_data(api_key, stocks, start_date, end_date):
    all_rows = []
    for company_names, stock in stocks.items():
        req = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&apikey={api_key}&outputsize=full')
        data_json = req.json()
        daily_data = data_json.get("Time Series (Daily)", {})

        for dates, info in daily_data.items():
            day_row = {
                "company": company_names,
                "stock": stock,
                "trading_day": dates,
                "open_price": info['1. open'],
                "high_price": info['2. high'],
                "low_price": info['3. low'],
                "close_price": info['4. close'],
                "volume": info['5. volume']
            }
            all_rows.append(day_row)
    
    df = pd.DataFrame(all_rows)

    # Convert 'trading_day' to datetime for accurate filtering and comparison
    df['trading_day'] = pd.to_datetime(df['trading_day'])

    if start_date:
        df = df[(df['trading_day'] >= start_date) & (df['trading_day'] <= end_date)]

    df = df.astype({
        "company": 'category',
        "stock": 'category',
        'open_price': 'float',
        'high_price': 'float',
        'low_price': 'float',
        'close_price': 'float',
        'volume': 'int'
    })

    data_to_sql(df)

app = func.FunctionApp()

@app.function_name(name="TimerTrigger1")
@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    
    get_historic_data(api_key, stocks, start_date, end_date)
