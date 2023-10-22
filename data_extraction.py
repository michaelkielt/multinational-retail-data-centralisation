'''
This class will work as a utility class, in it you will be creating 
methods that help extract data from different data sources.
The methods contained will be fit to extract data from a #
particular data source, these sources will include CSV files, 
an API and an S3 bucket
'''
from sqlalchemy import create_engine, inspect
from database_utils import DataBaseConnector

# Creates instance of database_utils class to connect
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
    
    
    
    def read_rds_tables(self, table_name):
        # Use db engine to execute a SELECT query on returned tables
        with self.engine.connect() as connection:
            query = f"SELECT * FROM {table_name}"
            result = connection.execute(query)
            data = result.fetchall()
            return data

# Creates data extractor instance with db engine
data_extractor = DataExtractor(db_engine)

# Lists tables in the connected database
tables = data_extractor.list_db_tables()
print("Tables in your database: ")
for table in tables:
    print(table)


# Read data from a specific table (replace 'your_table_name' with an actual table name)
table_data = data_extractor.read_rds_tables('legacy_store_details')
print("Data from this table: ")
for row in table_data:
    print(row)    
