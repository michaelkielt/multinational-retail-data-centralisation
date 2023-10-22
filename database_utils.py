import yaml

class DataBaseConnector:
    

    def read_db_creds():
        with open("db_creds.yaml", "r") as file:
            db_creds = yaml.safe_load(file)

        return db_creds
    
    def init_db_engine():
