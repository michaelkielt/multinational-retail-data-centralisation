import pandas as pd

class DataCleaning:
    def __init__(self, data):
        self.data = data
    
    def clean_user_data(self):
        # Convert 'date_of_birth' and 'join_date' columns to datetime
        self.data['date_of_birth'] = pd.to_datetime(self.data['date_of_birth'], errors='coerce')
        self.data['join_date'] = pd.to_datetime(self.data['join_date'], errors='coerce')
