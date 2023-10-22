import yaml
from sqlalchemy import create_engine

class DataBaseConnector:
    

    def read_db_creds():
        with open("db_creds.yaml", "r") as file:
            db_creds = yaml.safe_load(file)

        return db_creds
    

    def init_db_engine():
        db_config = DataBaseConnector.read_db_creds()
        db_url = create_engine(
            dialect="postgresql",
            username=db_config['aicore_admin'],
            password=db_config['AiCore2022'],
            host=db_config['data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'],
            port=db_config['5432'],
            database=db_config['postgres']
        )

        return db_url

        
        
