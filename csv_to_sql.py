import pandas as pd
import glob, os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# from sqlalchemy_utils import database_exists, create_database
import psycopg2

# Define your database connection parameters
with open("connection_info.json", "r") as file:
    conn_info = json.load(file)
    print("done reading file")

print(conn_info)

conn_string = f"postgresql://{conn_info['db_user']}:{conn_info['db_password']}@{conn_info['db_host']}:{conn_info['db_port']}/{conn_info['db_name']}"
# # Create a database engine with connection pooling
engine = create_engine(conn_string, pool_pre_ping=True)

session = sessionmaker(bind=engine)

print(session)

# Iterate through each CSV file in the current directory
for file in glob.glob("*.csv"):
    try:
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(file)

        # Use the table name as the filename without the extension
        table_name = os.path.splitext(file)[0]

        # Write the DataFrame to the database
        df.to_sql(table_name, engine, index=False, if_exists="replace")

        print(f"Table '{table_name}' imported successfully.")
    except Exception as e:
        print(f"Error importing '{file}': {e}")
