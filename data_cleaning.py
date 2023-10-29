import pandas as pd
import numpy as np
import datetime as dt
import re

class DataCleaning:
    def __init__(self, data):
        self.data = data
    
    def clean_user_data(self):
        # Replace "NULL" string objects with actual NaN values then drop them.
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        # Function to convert worded dates to 'YYYY-MM-DD'.
        def convert_worded_date(worded_date):
            if re.match(r'\d{4} \w+ \d{2}', worded_date):
                parts = worded_date.split()
                year = parts[0]
                month = parts[1]
                day = parts[2]
                return f"{year}-{month}-{day}"
            else:
                return worded_date

        # Apply the worded date conversion function to the date column.
        self.data['join_date'] = self.data['join_date'].apply(convert_worded_date)
        self.data['date_of_birth'] = self.data['date_of_birth'].apply(convert_worded_date)
        
        # Convert 'date_of_birth' and 'join_date' columns to datetime.
        self.data['date_of_birth'] = pd.to_datetime(self.data['date_of_birth'], errors='coerce')
        self.data['join_date'] = pd.to_datetime(self.data['join_date'], errors='coerce')

        # Format both columns to output in the same format.
        self.data['date_of_birth'] = self.data['date_of_birth'].dt.strftime('%Y-%m-%d')
        self.data['join_date'] = self.data['join_date'].dt.strftime('%Y-%m-%d')

        # Define a regular expression pattern to match invalid values found in data.
        # This matches on strings containing uppercase letters and numbers of 10 chars in length.
        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        # Define a function to check if any column in a row contains the pattern of 10. 
        # alphanumeric characters.
        def contains_pattern(row):
            for value in row:
                if re.search(pattern, str(value)):
                    return True
            return False

        # Apply the function to each row and keep rows that don't contain the pattern.
        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        # Function to clean phone numbers.
        def clean_phone_number(phone_num):
            # Remove non-numeric characters except 'x'.
            phone_num = re.sub(r'[^0-9x]', '', phone_num)

            # Remove leading '0' or '+'.
            phone_num = re.sub(r'^[0+]', '', phone_num)

            # Remove 'x' and anything after it.
            phone_num = re.sub(r'x.*$', '', phone_num)

            return phone_num

        # Apply the cleaning function to the 'phone_number' column.
        self.data.loc[:, 'phone_number'] = self.data['phone_number'].apply(clean_phone_number)

        # Replacing the newline characters in the address column with a space.
        self.data.loc[:, 'address'] = self.data['address'].str.replace('\n', ' ')



        return self.data
    
    def clean_card_data(self):
        # Replace "NULL" string objects with actual NaN values then drop them.
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        # Function to check if a date is in 'YYYY-MM-DD' format.
        def is_valid_date(date):
            try:
                dt.datetime.strptime(date, '%Y-%m-%d')
                return True
            except ValueError:
                return False

        # Filter out rows with invalid date formats in 'date_payment_confirmed'.
        self.data = self.data[self.data['date_payment_confirmed'].apply(is_valid_date)]

        # Validate and clean 'card_number' column.
        def validate_card_number(card_num):
            # Regular expression pattern for card numbers with lengths in the range 12-19.
            pattern = r'^\d{12,19}$'
            return bool(re.match(pattern, str(card_num)))

        self.data['is_valid_card_number'] = self.data['card_number'].apply(validate_card_number)

        # Remove rows with invalid card numbers.
        self.data = self.data[self.data['is_valid_card_number']]

        # Drop the 'is_valid_card_number' column as it's no longer needed.
        self.data.drop(columns=['is_valid_card_number'], inplace=True)

        return self.data
    

    def clean_store_data(self):
        # Overwrite 'lat' column with 'latitude' values.
        self.data['lat'] = self.data['latitude']

        # Drop the original 'latitude' column.
        self.data.drop(columns=['latitude'], inplace=True)

        # Rename 'lat' column to 'latitude'.
        self.data.rename(columns={'lat': 'latitude'}, inplace=True)
        
        
        # Replace "NULL" string objects with actual NaN values then drop them.
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        # Function to check if a date is in 'YYYY-MM-DD' format.
        def is_valid_date(date):
            try:
                dt.datetime.strptime(date, '%Y-%m-%d')
                return True
            except ValueError:
                return False

        # Filter out rows with invalid date formats in 'opening_date'.
        self.data = self.data[self.data['opening_date'].apply(is_valid_date)]

        # Define a regular expression pattern to match invalid values found in data.
        # This matches on strings containing uppercase letters and numbers of 10 chars in length.
        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        # Define a function to check if any column in a row contains the pattern of 10 
        # alphanumeric characters.
        def contains_pattern(row):
            for value in row:
               if re.search(pattern, str(value)):
                   return True
            return False

        # Apply the function to each row and keep rows that don't contain the pattern.
        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        # Replacing the newline characters in the address column with a space.
        self.data.loc[:, 'address'] = self.data['address'].str.replace('\n', ' ')

        # Remove the 'ee' prefix from the 'continent' column using .loc.
        self.data.loc[:, 'continent'] = self.data['continent'].str.replace('ee', '')

        # Return cleaned data in dataframe.
        return self.data
    

    def convert_product_weights(self, products_df):
        # Create a copy of the DataFrame to avoid modifying the original.
        converted_weights_df = products_df.copy()

        # Define a function to convert weights to kg.
        def convert_to_kg(weight_str):
            # Extract numeric value and unit (e.g., "100 ml" -> ("100", "ml")).
            match = re.match(r"([\d.]+)\s*(\w+)", str(weight_str))
            if match:
                value, unit = match.groups()
                value = float(value)

                # Convert units to kg using a rough estimate (1 ml = 1 g).
                if unit == "g":
                    result = value / 1000  # Convert grams to kg.
                elif unit == "ml":
                    result = value / 1000  # Convert milliliters to kg.
                else:
                    result = value  # Assume other units are already in kg.

                # Round the result to 4 decimal places.
                return round(result, 4)

            return None  # Return None for invalid or missing data.

        # Apply the conversion function to the 'weight' column.
        converted_weights_df['weight'] = converted_weights_df['weight'].apply(convert_to_kg)

        return converted_weights_df
    
    def clean_products_data(self):
        # Rename the first column to 'index'.
        self.data.rename(columns={self.data.columns[0]: 'index'}, inplace=True)

        # Replace "NULL" string objects with actual NaN values then drop them.
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        # Function to check if a date is in 'YYYY-MM-DD' format.
        def is_valid_date(date):
            try:
                dt.datetime.strptime(date, '%Y-%m-%d')
                return True
            except ValueError:
                return False

        # Filter out rows with invalid date formats in 'opening_date'.
        self.data = self.data[self.data['date_added'].apply(is_valid_date)]

        # Define a regular expression pattern to match invalid values found in data.
        # This matches on strings containing uppercase letters and numbers of 10 chars in length.
        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        # Define a function to check if any column in a row contains the pattern of 10. 
        # alphanumeric characters.
        def contains_pattern(row):
            for value in row:
               if re.search(pattern, str(value)):
                   return True
            return False

        # Apply the function to each row and keep rows that don't contain the pattern.
        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        return self.data
    
    def clean_orders_data(self):
        # Remove the specified columns.
        columns_to_remove = ['first_name', 'last_name', '1']
        self.data = self.data.drop(columns=columns_to_remove, errors='ignore')

        # Convert 'card_number' column to string and remove commas.
        self.data['card_number'] = self.data['card_number'].astype(str).str.replace(',', '', regex=True)

        # Validate and clean 'card_number' column.
        def validate_card_number(card_num):
            # Regular expression pattern for card numbers with lengths in the range 12-19.
            pattern = r'^\d{12,19}$'
            return bool(re.match(pattern, str(card_num)))

        self.data['is_valid_card_number'] = self.data['card_number'].apply(validate_card_number)

        # Remove rows with invalid card numbers.
        self.data = self.data[self.data['is_valid_card_number']]

        # Drop the 'is_valid_card_number' column as it's no longer needed.
        self.data.drop(columns=['is_valid_card_number'], inplace=True)

        # Define a regular expression pattern to match invalid values found in data.
        # This matches on strings containing uppercase letters and numbers of 10 chars in length.
        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        # Define a function to check if any column in a row contains the pattern of 10. 
        # alphanumeric characters.
        def contains_pattern(row):
            for value in row:
               if re.search(pattern, str(value)):
                   return True
            return False

        # Apply the function to each row and keep rows that don't contain the pattern.
        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        return self.data

    
    def clean_date_events_data(self):
        # Replace "NULL" string objects with actual NaN values then drop them.
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        # Define a regular expression pattern to match invalid values found in data.
        # This matches on strings containing uppercase letters and numbers of 10 chars in length.
        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        # Define a function to check if any column in a row contains the pattern of 10.
        # alphanumeric characters.
        def contains_pattern(row):
            for value in row:
               if re.search(pattern, str(value)):
                   return True
            return False

        # Apply the function to each row and keep rows that don't contain the pattern.
        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        # Convert "timestamp" column to datetime format.
        # Convert 'timestamp' column to datetime with format and remove the date part.
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'], format='%H:%M:%S', errors='coerce')
        self.data['timestamp'] = self.data['timestamp'].dt.time

        # Validate "month," "year," and "day" columns.
        valid_months = list(range(1, 13))  # Months are represented numerically (1-12).
        self.data = self.data[self.data['month'].str.strip().astype(int).isin(valid_months)]
        self.data = self.data[self.data['year'].str.isnumeric()]
        self.data = self.data[self.data['day'].str.isnumeric()]
        self.data = self.data[self.data['day'].astype(int).between(1, 31, inclusive='both')]

        # Define a reasonable range for the "year" column. 
        earliest_year = 1900  
        current_year = dt.datetime.now().year
        self.data = self.data[self.data['year'].astype(int).between(earliest_year, current_year, inclusive='both')]

        return self.data