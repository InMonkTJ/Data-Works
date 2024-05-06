import requests
import pandas as pd
import yaml

path_to_file = "../lib/cc.yaml"

with open(path_to_file, "r") as file:
    config = yaml.safe_load(file)

api_key = config["Api"]["Alphavantage"]
stocks = config["Companies_bulk"]["Names"]


def get_historic_data(api_key, stocks):
    all_rows = []
    for company_names, stock in stocks.items():
        req = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize=full'.format(stock, api_key))
        data_json = req.json()
        daily_data = data_json["Time Series (Daily)"]
        
        for dates, info in daily_data.items():
            day_row = { "Company": company_names,
                       "Stock": stock,
                        "Day" : dates,
                        "Open Price" : info['1. open'],
                        "High" : info['2. high'],
                        "Low": info['3. low'],
                        "Close": info['4. close'],
                        "Volume": info['5. volume']
                    }
            all_rows.append(day_row)
    df = pd.DataFrame(all_rows)
    df.astype({"Company": 'category', "Stock": 'category', 'Day': 'datetime64[ns]', 'Open Price': 'float', 'High': 'float', 'Low': 'float', 'Close': 'float', 'Volume': 'int' })
    print(df.dtypes)
    return df

cc = get_historic_data(api_key,stocks)

print(cc)
