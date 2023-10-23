import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine, inspect, text
from database_utils import DataBaseConnector
from data_cleaning import DataCleaning

# Creates instance of DatabaseConnecter class to connect
# to db and generate engine
db_connector = DataBaseConnector()
db_engine = db_connector.init_db_engine()

class DataExtractor: 
    def __init__(self, db_engine):
        self.engine = db_engine

    def list_db_tables(self):
        # Creates Inspector object to list table names
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names
    
    
    def read_rds_table(self, table_name):
        # Create a SQLAlchemy text object with the sql query
        query = text(f"SELECT * FROM {table_name}")

        # Use db engine to execute the query on one of returned tables
        with self.engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()
            
        # Convert the data to a dataframe
        df = pd.DataFrame(data)
        return df

# Creates data extractor instance with db engine
data_extractor = DataExtractor(db_engine)

# Lists tables in the connected database
tables = data_extractor.list_db_tables()
print("Tables in your database: ")
for table in tables:
    print(table)


# Read data from a specific table
legacy_user_data = data_extractor.read_rds_table('legacy_users')

# Now, create an instance of the DataCleaning class and pass the table_data for cleaning
data_cleaner = DataCleaning(legacy_user_data)

# Call the clean_user_data method to clean the data
cleaned_data = data_cleaner.clean_user_data()
print(cleaned_data.info())

# Upload cleaned data to postgres database
db_connector.upload_to_db(cleaned_data, "dim_user")



 

