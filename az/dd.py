from sqlalchemy import create_engine
import pandas as pd
import yaml






connection = (
    "mssql+pyodbc://theboy:ovoxo123/@conf.database.windows.net/sen-data"
    "?driver=ODBC Driver 17 for SQL Server&Command Timeout=120&connect_timeout=600"
)


engine = create_engine(connection, echo=True)

def data_to_sql(data):
    df = pd.DataFrame(data)

    df.to_sql("stocks", con=engine,index=False, if_exists='append')