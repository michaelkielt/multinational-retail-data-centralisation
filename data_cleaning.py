import pandas as pd
import numpy as np
import datetime as dt
import re

class DataCleaning:
    def __init__(self, data):
        self.data = data
    
    def clean_user_data(self):
        # Replace "NULL" string objects with actual NaN values then drop them
        self.data.replace("NULL", np.nan, inplace=True)
        self.data.dropna(inplace=True)

        # Define a function to convert worded dates to 'YYYY-MM-DD'
        def convert_worded_date(worded_date):
            if re.match(r'\d{4} \w+ \d{2}', worded_date):
                parts = worded_date.split()
                year = parts[0]
                month = parts[1]
                day = parts[2]
                return f"{year}-{month}-{day}"
            else:
                return worded_date

        # Apply the worded date conversion function to the date column
        self.data['join_date'] = self.data['join_date'].apply(convert_worded_date)
        self.data['date_of_birth'] = self.data['date_of_birth'].apply(convert_worded_date)
        
        # Convert 'date_of_birth' and 'join_date' columns to datetime
        self.data['date_of_birth'] = pd.to_datetime(self.data['date_of_birth'], errors='coerce')
        self.data['join_date'] = pd.to_datetime(self.data['join_date'], errors='coerce')

        # Format both columns to output in the same format
        self.data['date_of_birth'] = self.data['date_of_birth'].dt.strftime('%Y-%m-%d')
        self.data['join_date'] = self.data['join_date'].dt.strftime('%Y-%m-%d')

        # Define a regular expression pattern to match invalid values found in data
        # This matches on strings containing uppercase letters and numbers of 10 chars in length
        pattern = r'^[A-Z0-9][A-Za-z0-9]{9}$'

        # Define a function to check if any column in a row contains the pattern
        def contains_pattern(row):
            for value in row:
                if re.search(pattern, str(value)):
                    return True
            return False

        # Apply the function to each row and keep rows that don't contain the pattern
        self.data = self.data[~self.data.apply(contains_pattern, axis=1)]

        # Function to clean phone numbers
        def clean_phone_number(phone_num):
            # Remove non-numeric characters except 'x'
            phone_num = re.sub(r'[^0-9x]', '', phone_num)

            # Remove leading '0' or '+'
            phone_num = re.sub(r'^[0+]', '', phone_num)

            # Remove 'x' and anything after it
            phone_num = re.sub(r'x.*$', '', phone_num)

            return phone_num

        # Apply the cleaning function to the 'phone_number' column
        self.data.loc[:, 'phone_number'] = self.data['phone_number'].apply(clean_phone_number)

        # Replacing the newline characters in the address column with a space
        self.data.loc[:, 'address'] = self.data['address'].str.replace('\n', ' ')



        return self.data

    


    