import logging
import azure.functions as func
import requests
import pandas as pd
import yaml
import argparse
import datetime
from sqlalchemy import create_engine
import pandas as pd
import yaml

# Get the API key and the stocks to bulk import
api_key = 'HA2L8ODLDJL15IXH'
stocks = {
    "Chevron Corporation": "CVX",
    "ExxonMobil": "XOM",
    "Shell plc": "SHEL"
}

def data_to_sql(data):
    connection = (
        "mssql+pyodbc://theboy:ovoxo123/@conf.database.windows.net/sen-data?"
        "driver=ODBC Driver 17 for SQL Server&Command Timeout=120&connect_timeout=600"
    )
    engine = create_engine(connection, echo=True)
    data.to_sql("stocks", con=engine, index=False, if_exists='append')

def get_historic_data(api_key, stocks):
    all_rows = []
    for company_name, stock in stocks.items():
        req = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&apikey={api_key}&outputsize=full')
        data_json = req.json()
        daily_data = data_json["Time Series (Daily)"]

        for date, info in daily_data.items():
            day_row = {
                "company": company_name,
                "stock": stock,
                "trading_day": date,
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

    startfrom_str = "12/05/2024"
    endto_str = "14/05/2024"
    startfrom = datetime.datetime.strptime(startfrom_str, "%d/%m/%Y")
    endto = datetime.datetime.strptime(endto_str, "%d/%m/%Y")
    df = df[(df['trading_day'] >= startfrom) & (df['trading_day'] <= endto)]

    # Change the datatype of the columns and ensure the change is applied
    df = df.astype({
        "company": 'category',
        "stock": 'category',
        'open_price': 'float',
        'high_price': 'float',
        'low_price': 'float',
        'close_price': 'float',
        'volume': 'int'
    })

    # Import to the database
    data_to_sql(df)



app = func.FunctionApp()

@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    get_historic_data(api_key, stocks)

    logging.info('Python timer trigger function executed.')