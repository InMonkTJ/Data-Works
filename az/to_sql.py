from sqlalchemy import create_engine
import pandas as pd
import yaml
import os
from dotenv import load_dotenv

def details(config):
    load_dotenv()
    db_config = config['Sql_connections']['sen_data']
    username = db_config['username']
    password = db_config['password']
    server = db_config['server']
    database = db_config['database']
    return username, password, server, database

def get_conn_info(data, n):
    if n == 0:  
        load_dotenv()
    else:
        load_dotenv('TimerTrigger1/.env')
        
    username = os.getenv('SEN_DATA_USERNAME')
    password = os.getenv('SEN_DATA_PASSWORD')
    server = os.getenv('SEN_DATA_SERVER')
    database = os.getenv('SEN_DATA_DATABASE')

    connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}?timeout=600"

    # Create the SQLAlchemy engine
    try:
        engine = create_engine(connection_string, echo=True)
    except Exception as e:
        raise RuntimeError(f"Error creating SQLAlchemy engine: {e}")

    # Load data to SQL
    data_to_sql(data, engine)

def data_to_sql(data, engine):
    df = pd.DataFrame(data)
    try:
        df.to_sql("stocks", con=engine, index=False, if_exists='append')
    except Exception as e:
        raise RuntimeError(f"Error writing data to SQL: {e}")