from sqlalchemy import create_engine
import pandas as pd
import yaml

# Load YAML configuration
with open('TimerTrigger1/dd.yaml', "r") as file:
    config = yaml.safe_load(file)

# Extract database configuration
db_config = config['Sql_connections']['sen_data']

username = db_config['username']
password = db_config['password']
server = db_config['server']
database = db_config['database']

# Create connection string using pymssql
connection = (
    f"mssql+pymssql://{username}:{password}@{server}/{database}?timeout=600"
)

# Create SQLAlchemy engine
engine = create_engine(connection, echo=True)

def data_to_sql(data):
    # Convert data to DataFrame
    df = pd.DataFrame(data)
    
    # Insert data into the "stocks" table
    df.to_sql("stocks", con=engine, index=False, if_exists='append')
