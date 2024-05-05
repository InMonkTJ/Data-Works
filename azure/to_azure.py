import pandas as pd
from sqlalchemy import create_engine

server_name = "conf.database.windows.net"
database_name = "sen-data"
username = "theboy"
password = "ovoxo123/"
driver = "ODBC Driver 17 for SQL Server"

connection_string = f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver={driver}"
engine = create_engine(connection_string, echo=True)

df = pd.read_csv("cars.csv")

df.to_sql('tt', con=engine, index=False, if_exists='replace')


    