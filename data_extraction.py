from sqlalchemy import create_engine, inspect, text
from database_utils import DataBaseConnector
from data_cleaning import DataCleaning
from io import StringIO
import pandas as pd
import numpy as np
import requests 
import tabula
import boto3
import json



# Creates instance of DatabaseConnecter class to connect.
# to db and generate engine.
db_connector = DataBaseConnector()
db_engine = db_connector._init_db_engine()

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
        query = text(f'SELECT * FROM {table_name}')

        with self.engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()
            
        table_df = pd.DataFrame(data)
        return table_df
    
    
    def _retrieve_pdf_data(self, pdf_path):
        """
        Retrieve data from a PDF document and return it as a Pandas DataFrame.

        Args:
            pdf_path (str): Path to the PDF file to extract data from.

        Returns:
            Pandas DataFrame containing the extracted data.
        """
        pdf_data = tabula.read_pdf(pdf_path, pages='all')
        
        if pdf_data:
            combined_df = pd.concat(pdf_data) # Combine DataFrames into a single DataFrame.
            return combined_df
        else:
            return None
        

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

    
    def _retrieve_stores_data(self, base_endpoint, headers, total_stores=451):
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

        for store_number in range(1, total_stores + 1):
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






"""
# Creates data extractor instance with db engine
data_extractor = DataExtractor(db_engine)

# Lists tables in the connected database
tables = data_extractor._list_db_tables()
print('Tables in your database: ')
for table in tables:
    print(table)

Read data from a specific table
legacy_user_data = data_extractor._read_rds_table('legacy_users')

Creates an instance of the DataCleaning class and passes the table_data for cleaning
user_data_cleaner = DataCleaning(legacy_user_data)

Call the clean_user_data method to clean the data
cleaned_user_data = user_data_cleaner._clean_user_data()
print(cleaned_user_data.info())
print(cleaned_user_data['date_of_birth'].dtypes)
print(cleaned_user_data['join_date'].dtypes)

Upload cleaned data to the Postgres database
db_connector._upload_to_db(cleaned_user_data, "dim_user")

card_data_extractor = DataExtractor(db_engine)
card_data = card_data_extractor._retrieve_pdf_data("card_details.pdf")
card_data_cleaner = DataCleaning(card_data)
cleaned_card_data = card_data_cleaner._clean_card_data()
print(cleaned_card_data)
db_connector._upload_to_db(cleaned_card_data, "dim_card_details")

headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
no_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
base_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
store_data_extractor = DataExtractor(db_engine)

total_stores = store_data_extractor._list_number_of_stores(no_of_stores_endpoint, headers)
if total_stores is not None:
    print('Data extracted from the endpoint:')
    print(total_stores)
else:
    print('Unable to retrieve data from the endpoint.')

all_stores_df = store_data_extractor._retrieve_stores_data(base_store_endpoint, headers)
num_rows, num_columns = all_stores_df.shape
print(f"Number of rows: {num_rows}")
print(f"Number of columns: {num_columns}")
print(all_stores_df.info())
all_stores_df.to_csv('stores_csv.csv', index=False)

store_data_cleaner = DataCleaning(all_stores_df)
cleaned_store_data = store_data_cleaner._clean_store_data()
print(cleaned_store_data.info())
cleaned_store_data.to_excel('cleaned_store_data.xlsx', index=False)

Upload cleaned store data to the database
db_connector._upload_to_db(cleaned_store_data, "dim_store_details")

s3_address = 's3://data-handling-public/products.csv'
product_data_df = data_extractor._extract_csv_from_s3(s3_address)
print(product_data_df.info())
product_data_df.to_excel('product_data.xlsx', index=False)

weights = DataCleaning(product_data_df)
converted_weights_df = weights._convert_product_weights(product_data_df)
print(converted_weights_df.info())

product_data_cleaner = DataCleaning(converted_weights_df)
cleaned_product_data = product_data_cleaner._clean_products_data()
print(cleaned_product_data.info())
cleaned_product_data.to_excel('cleaned_product_data.xlsx', index=False)

Upload cleaned product data to the database
db_connector.upload_to_db(cleaned_product_data, 'dim_products')

order_data = data_extractor._read_rds_table('orders_table')
print(order_data.info())

order_data_cleaner = DataCleaning(order_data)
cleaned_orders_data = order_data_cleaner._clean_orders_data()
print(cleaned_orders_data.info())

db_connector._upload_to_db(cleaned_orders_data, 'orders_table')

date_data_s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'


date_events_data = data_extractor._extract_json_from_s3(date_data_s3_address)
print(date_events_data.info())

date_events_data_cleaner = DataCleaning(date_events_data)
cleaned_date_events_data = date_events_data_cleaner._clean_date_events_data()
print(cleaned_date_events_data.info())

db_connector._upload_to_db(cleaned_date_events_data, 'dim_date_times')
"""
