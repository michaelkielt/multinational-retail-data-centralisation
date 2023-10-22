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

        
        
