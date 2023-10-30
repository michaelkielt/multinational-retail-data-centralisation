import yaml
from sqlalchemy import create_engine

class DataBaseConnector:
    """
    A class for connecting to and interacting with databases.

    This class provides methods for reading database credentials, initializing database engines, and uploading data to a database.

    Attributes:
        None

    Methods:
        _read_db_creds(self) -> dict:
            Reads database credentials from a YAML file and returns them as a dictionary.

        _init_db_engine(self) -> sqlalchemy.engine.base.Connection:
            Initializes a SQLAlchemy database engine using the retrieved credentials for a remote database.

        _init_local_db_engine(self) -> sqlalchemy.engine.base.Connection:
            Initializes a SQLAlchemy database engine for a local database with pre-defined credentials.

        _upload_to_db(self, df: pandas.DataFrame, table_name: str):
            Uploads a cleaned Pandas DataFrame to a PostgreSQL database table, replacing the existing data.

    """
    
    def _read_db_creds(self):
        """
        Read in the db_creds YAML file and output as a dictionary.

        Returns:
            dict: A dictionary containing database credentials.
        """
        with open("db_creds.yaml", "r") as file:
            db_creds = yaml.safe_load(file)

        return db_creds
    

    def _init_db_engine(self):
        """
        Create a database engine based on the database credentials read from db_creds.yaml.

        Returns:
            sqlalchemy.engine.base.Connection: An initialized SQLAlchemy database engine for a remote database.
        """
        db_config = self._read_db_creds()
        db_url = f"postgresql+psycopg2://{db_config['RDS_USER']}:{db_config['RDS_PASSWORD']}@{db_config['RDS_HOST']}:{db_config['RDS_PORT']}/{db_config['RDS_DATABASE']}"

        engine = create_engine(db_url)
        return engine
    
    
    def _init_local_db_engine(self):
        """
        Create a database engine for a local PostgreSQL database with predefined credentials.

        Returns:
            sqlalchemy.engine.base.Connection: An initialized SQLAlchemy database engine for a local database.
        """
        db_url = "postgresql+psycopg2://postgres:t0ymach1ne@localhost:5432/sales_data"
        engine = create_engine(db_url)
        return engine
    

    def _upload_to_db(self, df, table_name):
        """
        Upload a Pandas DataFrame to a PostgreSQL database table, replacing existing data.

        Args:
            df (pandas.DataFrame): The cleaned data in the form of a Pandas DataFrame.
            table_name (str): The name of the database table to upload the data to.

        Returns:
            None
        """
        try:
            local_engine = self._init_local_db_engine()
            df.to_sql(table_name, local_engine, if_exists='replace', index=False)
            print(f"Uploaded DataFrame to table: {table_name}")
        except Exception as e:
            print(f"An error occurred while uploading the DataFrame to the local database: {str(e)}") 


        
        
