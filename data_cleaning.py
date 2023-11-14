import pandas as pd
import numpy as np
import datetime as dt
import re

class DataCleaning:
    """
    A class for cleaning and preprocessing data within a DataFrame.

    This class provides methods for cleaning user data, card data, store data, product data, order data, and date events data in a Pandas DataFrame.

    Attributes:
        data (pandas.DataFrame): The input DataFrame to be cleaned and processed.

    Methods:
        _clean_user_data(self) -> pandas.DataFrame:
            Clean user data within the DataFrame. Replace "NULL" values, convert dates, clean phone numbers, and more.

        _clean_card_data(self) -> pandas.DataFrame:
            Clean card data within the DataFrame. Replace "NULL" values, validate and clean card numbers.

        _clean_store_data(self) -> pandas.DataFrame:
            Clean store data within the DataFrame. Replace "NULL" values, convert dates, clean addresses and 'continent' column.

        _convert_product_weights(self, products_df: pandas.DataFrame) -> pandas.DataFrame:
            Convert product weights to kilograms in a separate DataFrame.

        _clean_products_data(self) -> pandas.DataFrame:
            Clean products data within the DataFrame. Rename columns, replace "NULL" values, and filter invalid rows with erroneous 
            values.

        _clean_orders_data(self) -> pandas.DataFrame:
            Clean orders data within the DataFrame. Remove specified columns, validate and clean 'card_number,' and filter invalid rows 
            with erroneous values.

        _clean_date_events_data(self) -> pandas.DataFrame:
            Clean date events data within the DataFrame. Replace "NULL" values, filter invalid rows rows with erroneous values, convert 
            "timestamp" column, and validate date-related columns.

    """
    
    def __init__(self, data):
        """
        Initialise the DataCleaning class with a DataFrame.

        Parameters:
        data (pandas.DataFrame): The input DataFrame to be cleaned.
        """
        self.data = data
    
    
    def _clean_user_data(self):
        """
        Clean user data in the DataFrame.

        - Replaces "NULL" string objects with actual NaN values in 'user_uuid' and drops them.
        - Converts worded dates to 'YYYY-MM-DD' format.
        - Cleans phone numbers.
        - Replaces newline characters in the address column with spaces.

        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        self.data['user_uuid'] = self.data['user_uuid'].replace("NULL", np.nan)
        self.data.dropna(subset=['user_uuid'], inplace=True)


        def convert_worded_date(worded_date):
        # Check if the date matches the format 'YYYY Month DD' or 'Month YYYY DD'.
            match = re.match(r'(\d{4})-(\w+)-(\d{2})|(\d{4})-(\d{2})-(\w+)', worded_date)
    
            if match:
                year = match.group(1) or match.group(4)
                month = match.group(2) or match.group(3)
                day = match.group(3) or match.group(6)
                return f"{year}-{month}-{day}"
            else:
                return worded_date

        self.data['join_date'] = self.data['join_date'].apply(convert_worded_date)
        self.data['date_of_birth'] = self.data['date_of_birth'].apply(convert_worded_date)

        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'
 
        def contains_pattern(row):
            if re.search(pattern, str(row['user_uuid'])):
                    return True
            return False

        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        def clean_phone_number(phone_num):
        # Remove 'x' and anything after it, and remove the '.'.
            phone_num = re.sub(r'x.*', '', phone_num)
            phone_num = phone_num.replace('.', '-')

            return phone_num

        self.data.loc[:, 'phone_number'] = self.data['phone_number'].apply(clean_phone_number)

        self.data.loc[:, 'address'] = self.data['address'].str.replace('\n', ' ')

        return self.data
    
    
    def _clean_card_data(self):
        """
        Clean card data in the DataFrame.

        - Replaces "NULL" string objects in 'expiry_date' with actual NaN values and drops them.
        - Converts invalid date formats to 'YYYY-MM-DD' in 'date_payment_confirmed'.
        - Validates and cleans 'card_number' column.

        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        self.data['expiry_date'] = self.data['expiry_date'].replace("NULL", np.nan)
        self.data.dropna(inplace=True)

        def convert_worded_date(worded_date):
        # Check if the date matches the format 'YYYY Month DD' or 'Month YYYY DD'.
            match = re.match(r'(\d{4})-(\w+)-(\d{2})|(\d{4})-(\d{2})-(\w+)', worded_date)
    
            if match:
                year = match.group(1) or match.group(4)
                month = match.group(2) or match.group(3)
                day = match.group(3) or match.group(6)
                return f"{year}-{month}-{day}"
            else:
                return worded_date

        self.data['date_payment_confirmed'] = self.data['date_payment_confirmed'].apply(convert_worded_date)

        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'
 
        def contains_pattern(row):
            if re.search(pattern, str(row['expiry_date'])):
                    return True
            return False

        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        def clean_card_number(card_num):
        # Remove commas from the card number.
            return re.sub(',', '', str(card_num))

        self.data['card_number'] = self.data['card_number'].apply(clean_card_number)

        return self.data
    

    def _clean_store_data(self):
        """
        Clean store data in the DataFrame.

        - Overwrites 'lat' column with 'latitude' values.
        - Drops the original 'latitude' column.
        - Cleans the address and 'continent' columns.

        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        self.data['lat'] = self.data['latitude']

        self.data.drop(columns=['latitude'], inplace=True)

        self.data.rename(columns={'lat': 'latitude'}, inplace=True)

        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        def contains_pattern(row):
            if re.search(pattern, str(row['country_code'])):
                    return True
            return False

        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        self.data.loc[:, 'address'] = self.data['address'].str.replace('\n', ' ')

        self.data.loc[:, 'continent'] = self.data['continent'].str.replace('ee', '')
        
        self.data['staff_numbers'] = self.data['staff_numbers'].str.replace(r'[^0-9]', '')

        return self.data
    

    def _convert_product_weights(self, products_df):
        """
        Convert product weights to kilograms in the DataFrame.

        Parameters:
        products_df (pandas.DataFrame): The DataFrame containing product data.

        Returns:
        pandas.DataFrame: The DataFrame with weights converted to kilograms.
        """
        converted_weights_df = products_df.copy()

        def convert_to_kg(weight_str):
            # Extract numeric value and unit (e.g., "100 ml" -> ("100", "ml")).
            match = re.match(r"([\d.]+)\s*(\w+)", str(weight_str))
            if match:
                value, unit = match.groups()
                value = float(value)

                if unit == "g":
                    result = value / 1000 
                elif unit == "ml":
                    result = value / 1000 
                else:
                    result = value

                return round(result, 4)

            return None 

        converted_weights_df['weight'] = converted_weights_df['weight'].apply(convert_to_kg)

        return converted_weights_df
    
    def _clean_products_data(self):
        """
        Clean products data in the DataFrame.

        - Renames the first column to 'index'.
        - Replaces "NULL" string objects in 'product_code' with actual NaN values and drops them.
        - Converts invalid date formats to YYYY-MM-DD.
        
        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        self.data.rename(columns={self.data.columns[0]: 'index'}, inplace=True)

        self.data['product_code'] = self.data['product_code'].replace("NULL", np.nan)
        self.data.dropna(inplace=True)

        def convert_worded_date(worded_date):
        # Check if the date matches the format 'YYYY Month DD' or 'Month YYYY DD'.
            match = re.match(r'(\d{4})-(\w+)-(\d{2})|(\d{4})-(\d{2})-(\w+)', worded_date)
    
            if match:
                year = match.group(1) or match.group(4)
                month = match.group(2) or match.group(3)
                day = match.group(3) or match.group(6)
                return f"{year}-{month}-{day}"
            else:
                return worded_date

        self.data['date_added'] = self.data['date_added'].apply(convert_worded_date)

        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        def contains_pattern(row):
            if re.search(pattern, str(row['uuid'])):
                    return True
            return False

        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        return self.data
    
    def _clean_orders_data(self):
        """
        Clean orders data in the DataFrame.

        - Removes specified columns.
        - Converts 'card_number' column to string and removes commas.
        - Validates and cleans 'card_number' column.
        - Filters out rows with erroneous values.

        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        columns_to_remove = ['first_name', 'last_name', '1']
        self.data = self.data.drop(columns=columns_to_remove, errors='ignore')

        self.data['card_number'] = self.data['card_number'].astype(str).str.replace(',', '', regex=True)

        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        def contains_pattern(row):
            if re.search(pattern, str(row['date_uuid'])):
                    return True
            return False

        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        return self.data

    
    def _clean_date_events_data(self):
        """
        Clean date events data in the DataFrame.

        - Replaces "NULL" string objects with actual NaN values and drops them.
        - Filters out rows with invalid values.
        - Converts "timestamp" column to datetime format.
        - Validates "month," "year," and "day" columns.
        - Defines a reasonable range for the "year" column.

        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        def contains_pattern(row):
            for value in row:
               if re.search(pattern, str(value)):
                   return True
            return False

        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], format='%H:%M:%S', errors='coerce')
        self.data['timestamp'] = self.data['timestamp'].dt.time

        valid_months = list(range(1, 13)) 
        self.data = self.data[self.data['month'].str.strip().astype(int).isin(valid_months)]
        self.data = self.data[self.data['year'].str.isnumeric()]
        self.data = self.data[self.data['day'].str.isnumeric()]
        self.data = self.data[self.data['day'].astype(int).between(1, 31, inclusive='both')]
 
        earliest_year = 1900  
        current_year = dt.datetime.now().year
        self.data = self.data[self.data['year'].astype(int).between(earliest_year, current_year, inclusive='both')]

        return self.data