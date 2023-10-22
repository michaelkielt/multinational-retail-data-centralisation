'''
This class will work as a utility class, in it you will be creating 
methods that help extract data from different data sources.
The methods contained will be fit to extract data from a #
particular data source, these sources will include CSV files, 
an API and an S3 bucket
'''
from sqlalchemy import create_engine, inspect
from database_utils import DataBaseConnector

# Create instance of database_utils class to connect
# to db and generate engine
db_connector = DataBaseConnector()
db_engine = db_connector.init_db_engine()

class DataExtractor: 
    def __init__(self, db_engine):
        self.engine = db_engine

    def list_db_tables(self):
        # Create Inspector object to list table names
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names
    
    
    
    #def read_rds_tables():

data_extractor = DataExtractor(db_engine)
