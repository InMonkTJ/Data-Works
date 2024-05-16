import logging
import azure.functions as func
import requests
import pandas as pd
import yaml
from to_sql import data_to_sql
import datetime

path_to_file = "../lib/cc.yaml"

def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%d/%m/%Y")

def get_historic_data(api_key, stocks, startfrom, endto):
    all_rows = []
    for company_names, stock in stocks.items():
        req = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize=full'.format(stock, api_key))
        data_json = req.json()
        daily_data = data_json["Time Series (Daily)"]

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

    df['trading_day'] = pd.to_datetime(df['trading_day'])
    df = df[(df['trading_day'] >= startfrom) & (df['trading_day'] <= endto)]
    df = df.astype({"company": 'category', "stock": 'category', 'open_price': 'float', 'high_price': 'float', 'low_price': 'float', 'close_price': 'float', 'volume': 'int'})

    data_to_sql(df)

@func.HttpTrigger
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        startfrom = req.params.get('startfrom')
        endto = req.params.get('endto')

        if not startfrom:
            startfrom = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%d/%m/%Y")
        if not endto:
            endto = datetime.datetime.now().strftime("%d/%m/%Y")

        startfrom_date = parse_date(startfrom)
        endto_date = parse_date(endto)

        with open(path_to_file, "r") as file:
            config = yaml.safe_load(file)

        api_key = config["Api"]["Alphavantage"]
        stocks = config["Companies_bulk"]["Names"]

        get_historic_data(api_key, stocks, startfrom_date, endto_date)
        return func.HttpResponse(f"Data imported successfully from {startfrom} to {endto}.")
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
