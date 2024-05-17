from sqlalchemy import create_engine
import pandas as pd
import yaml
import os




with open('TimerTrigger1/dd.yaml', "r") as file:
    config = yaml.safe_load(file)


db_config = config['Sql_connections']['sen_data']

username = db_config['username']
password = db_config['password']
driver = db_config['driver']

connection = (
    f"mssql+pyodbc://{username}:{password}@{db_config['server']}/{db_config['database']}?driver={driver}&Command Timeout=120&connect_timeout=600"
)

engine = create_engine(connection, echo=True)

def data_to_sql(data):
    df = pd.DataFrame(data)

    df.to_sql("stocks", con=engine,index=False, if_exists='append')