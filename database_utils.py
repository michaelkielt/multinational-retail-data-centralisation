import yaml
from sqlalchemy import create_engine

class DataBaseConnector:
    
    # Read in the db_creds YAML and output as dict
    def read_db_creds(self):
        with open("db_creds.yaml", "r") as file:
            db_creds = yaml.safe_load(file)

        return db_creds
    
    # Create engine from the read_db_creds method
    def init_db_engine(self):
        db_config = self.read_db_creds()
        db_url = f"postgresql+psycopg2://{db_config['RDS_USER']}:{db_config['RDS_PASSWORD']}@{db_config['RDS_HOST']}:{db_config['RDS_PORT']}/{db_config['RDS_DATABASE']}"

        engine = create_engine(db_url)
        return engine
    
    # Separate function to generate an engine for the local database
    def init_local_db_engine(self):
        # Create an engine for the local database
        db_url = "postgresql+psycopg2://postgres:t0ymach1ne@localhost:5432/sales_data"
        engine = create_engine(db_url)
        return engine
    
    # Uploads cleaned data to postgres database
    def upload_to_db(self, df, table_name):
        try:
            # Use the local engine to upload the Pandas DataFrame to the specified table
            local_engine = self.init_local_db_engine()
            df.to_sql(table_name, local_engine, if_exists='replace', index=False)
            print(f"Uploaded DataFrame to table: {table_name}")
        except Exception as e:
            print(f"An error occurred while uploading the DataFrame to the local database: {str(e)}") 


        
        
