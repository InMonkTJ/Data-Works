from sqlalchemy import create_engine
import pandas as pd
import yaml
import os

def details(config):
    db_config = config['Sql_connections']['sen_data']
    username = db_config['username']
    password = db_config['password']
    server = db_config['server']
    database = db_config['database']
    return username, password, server, database

def get_conn_info(data, n):
    if n == 0:  
        config_path = 'dd.yaml'
    else:
        config_path = 'TimerTrigger1/dd.yaml'

    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as e:
        raise RuntimeError(f"Error parsing YAML file: {e}")

    # Get database connection details
    username, password, server, database = details(config)
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