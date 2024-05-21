import datetime
import logging
import azure.functions as func
import requests
import pandas as pd
import yaml
import os
from dotenv import load_dotenv
import sys



def get_historic_data(api_key, stocks, n, to_start = None, to_end=None):
    
    if n == 0:
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        from to_sql import get_conn_info
        
    else:
        from to_sql import get_conn_info
    
    if to_end is None:
        to_end = datetime.datetime.now()
    if to_start is None:
        to_start = to_end - datetime.timedelta(days=1)
    
    all_rows = []
    for company_names, stock in stocks.items(): 
        req = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize=full'.format(stock, api_key)) #Api connection url
        data_json = req.json()
        daily_data = data_json["Time Series (Daily)"]
        
        for dates, info in daily_data.items(): 
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


    df['trading_day'] = pd.to_datetime(df['trading_day'])
    df = df[(df['trading_day'] >= to_start) & (df['trading_day'] <= to_end)]
    df = df.astype({"company": 'category', "stock": 'category', 'open_price': 'float', 'high_price': 'float', 'low_price': 'float', 'close_price': 'float', 'volume': 'int'})
    
    get_conn_info(df, n)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    n = 1
    
    with open('TimerTrigger1/dd.yaml', "r") as file:
        config = yaml.safe_load(file)
    
    stocks = get_keys(config)
    
    load_dotenv('TimerTrigger1/.env')
    api_key = os.getenv('ALPHAVANTAGE_API_KEY')
    
    get_historic_data(api_key, stocks, n)
    
    
    
def get_keys(config):
    stocks = config["Companies_bulk"]["Names"]
    return stocks


if __name__ == '__main__':
    import argparse
    n = 0
    
    path_to_file = 'dd.yaml'
    
    with open(path_to_file, 'r') as file:
        config = yaml.safe_load(file)
    
    stocks = get_keys(config)
    
    load_dotenv()
    api_key = os.getenv('ALPHAVANTAGE_API_KEY')

    def parse_date(date_string):
        return datetime.datetime.strptime(date_string, "%d/%m/%Y")

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--startfrom', type=parse_date, default=datetime.datetime.now() - datetime.timedelta(days=2), help= "where do you want to start from")
    parser.add_argument('-e', '--endto', type=parse_date, default=datetime.datetime.now(), help= "where do you want to end to")

    args = parser.parse_args()
    
    to_start = args.startfrom
    to_end = args.endto
    
    get_historic_data(api_key, stocks, n, to_start, to_end)
    