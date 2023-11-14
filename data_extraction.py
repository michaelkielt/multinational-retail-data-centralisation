from sqlalchemy import create_engine, inspect, text
from database_utils import DataBaseConnector
from io import StringIO
import pandas as pd
import numpy as np
import requests 
import tabula
import boto3
import json



class DataExtractor:
    """
    Class for extracting data from various sources and converting it to Pandas DataFrames.

    Args:
        db_engine (sqlalchemy.engine.base.Connection): The database engine for connecting to the database.

    Methods:
        _list_db_tables(self):
            List the names of tables in the connected database.

        _read_rds_table(self, table_name):
            Read data from the specified RDS database table and return it as a Pandas DataFrame.

        _retrieve_pdf_data(self, pdf_path):
            Retrieve data from a PDF document and return it as a Pandas DataFrame.

        _list_number_of_stores(self, endpoint, headers):
            Retrieve the number of stores from a web service.

        _retrieve_stores_data(self, base_endpoint, headers, total_stores=451):
            Retrieve data for multiple stores from a web service and return it as a Pandas DataFrame.

        _extract_csv_from_s3(self, s3_address):
            Extract data from a CSV file on Amazon S3 and return it as a Pandas DataFrame.

        _extract_json_from_s3(self, s3_address):
            Extract data from a JSON file on Amazon S3 and return it as a Pandas DataFrame.
    """ 
    TOTAL_STORES = 451

    def __init__(self, db_engine):
        self.engine = db_engine

    
    def _list_db_tables(self):
        """
        List the names of tables in the connected database.

        Returns:
            List of table names.
        """
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names
    
    
    def _read_rds_table(self, table_name):
        """
        Read data from the specified RDS database table and return it as a Pandas DataFrame.

        Args:
            table_name (str): Name of the database table to read.

        Returns:
            Pandas DataFrame containing the data from the table.
        """
        try:
            query = text(f'SELECT * FROM {table_name}')
            with self.engine.connect() as connection:
                result = connection.execute(query)
                data = result.fetchall()
            table_df = pd.DataFrame(data)
            return table_df
        except Exception as e:
            print(f'Error reading RDS table {table_name}: {str(e)}')
            return pd.DataFrame()
    
    
    def _retrieve_pdf_data(self, pdf_path):
        """
        Retrieve data from a PDF document and return it as a Pandas DataFrame.

        Args:
            pdf_path (str): Path to the PDF file to extract data from.

        Returns:
            Pandas DataFrame containing the extracted data.
        """
        try:
            pdf_data = tabula.read_pdf(pdf_path, pages='all', stream=False)
            if pdf_data:
                combined_df = pd.concat(pdf_data)  # Combine DataFrames into a single DataFrame.
                return combined_df
            else:
                return pd.DataFrame()  # Return an empty DataFrame if no data is found.
        except Exception as e:
            print(f'Error retrieving PDF data: {str(e)}')
            return pd.DataFrame()
        

    def _list_number_of_stores(self, endpoint, headers):
        """
        Retrieve the number of stores from a web service.

        Args:
            endpoint (str): The URL of the web service.
            headers (dict): Headers to include in the HTTP request.

        Returns:
            The number of stores if available, or None if the request is unsuccessful or data is not found.
        """
        try:
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                no_of_store_data = response.json()
                if 'number_stores' in no_of_store_data:
                    return no_of_store_data['number_stores']
                else:
                    return None  # 'number_stores' key not found in the response.
            else:
                return None  # Request was unsuccessful.
        except Exception as e:
            print(f'Error: {e}')
            return None 

    
    def _retrieve_stores_data(self, base_endpoint, headers, total_stores=TOTAL_STORES):
        """
        Retrieve data for multiple stores from a web service and return it as a Pandas DataFrame.

        Args:
            base_endpoint (str): The base URL of the web service for store data.
            headers (dict): Headers to include in the HTTP request.
            total_stores (int): The total number of stores to retrieve data for.

        Returns:
            Pandas DataFrame containing the data for all stores.
        """
        all_stores_data = pd.DataFrame()

        for store_number in range(0, total_stores + 1):
            store_endpoint = f'{base_endpoint}{store_number}'
            try:
                response = requests.get(store_endpoint, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    store_data = pd.DataFrame(data, index=[store_number])
                    
                    if any(store_data.columns.str.contains(' ')):
                        print(f'Warning: Found spaces in column names for store {store_number}. Data might be inconsistent.')
                    
                    all_stores_data = pd.concat([all_stores_data, store_data])
                else:
                    print(f'Failed to retrieve data for store {store_number}')
            except Exception as e:
                print(f'Error: {e}')
                print(f'Failed to retrieve data for store {store_number}')

        return all_stores_data
    

    def _extract_csv_from_s3(self, s3_address):
        """
        Extract data from a CSV file from an Amazon S3 bucket and return it as a Pandas DataFrame.

        Args:
            s3_address (str): The S3 address (URL) of the CSV file.

        Returns:
            Pandas DataFrame containing the extracted data, or None if extraction fails.
        """
        s3 = boto3.client('s3')

        try:
            s3_bucket = s3_address.split('/')[2]
            s3_object = '/'.join(s3_address.split('/')[3:])
            response = s3.get_object(Bucket=s3_bucket, Key=s3_object)
            data = response['Body'].read().decode('utf-8')

            s3_df = pd.read_csv(StringIO(data))
            return s3_df
        except Exception as e:
            print(f'Error extracting data from S3: {str(e)}')
            return None

    
    def _extract_json_from_s3(self, s3_address):
        """
        Extract data from a JSON file from an Amazon S3 bucket and return it as a Pandas DataFrame.

        Args:
            s3_address (str): The S3 address (URL) of the JSON file.

        Returns:
            Pandas DataFrame containing the extracted data, or None if extraction fails.
        """
        s3 = boto3.client('s3')

        try:
            s3_bucket = s3_address.split('/')[2]
            s3_object = '/'.join(s3_address.split('/')[3:])
            response = s3.get_object(Bucket=s3_bucket, Key=s3_object)
            data = response['Body'].read().decode('utf-8')

            json_data = json.loads(data)

            s3_df = pd.DataFrame(json_data)

            return s3_df
        except Exception as e:
            print(f'Error extracting data from S3: {str(e)}')
            return None

