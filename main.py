from data_cleaning import DataCleaning
from database_utils import DataBaseConnector
from data_extraction import DataExtractor

class DataHandlingMain:
    """Main class for handling data extraction and uploading to a centralised database."""

    def __init__(self):
        """Initialise the DataHandlingMain instance.

        Creates an instance of DatabaseConnector class to connect
        to the database and generate an engine.
        """
        self.db_connector = DataBaseConnector()
        self.db_engine = self.db_connector._init_db_engine()

    def extract_and_upload_user_data(self):
        """Extract user data, clean it, and upload it to the database."""
        data_extractor = DataExtractor(self.db_engine)
        legacy_user_data = data_extractor._read_rds_table('legacy_users')

        user_data_cleaner = DataCleaning(legacy_user_data)
        cleaned_user_data = user_data_cleaner._clean_user_data()

        self.db_connector._upload_to_db(cleaned_user_data, "dim_users_table")

    def extract_and_upload_card_data(self):
        """Extract card data, clean it, and upload it to the database."""
        card_data_extractor = DataExtractor(self.db_engine)
        card_data = card_data_extractor._retrieve_pdf_data("card_details.pdf")

        card_data_cleaner = DataCleaning(card_data)
        cleaned_card_data = card_data_cleaner._clean_card_data()

        self.db_connector._upload_to_db(cleaned_card_data, "dim_card_details")

    def extract_and_upload_store_data(self):
        """Extract store data, clean it, and upload it to the database."""
        store_data_extractor = DataExtractor(self.db_engine)

        headers = self.db_connector._read_headers()

        if headers:
            base_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

            all_stores_df = store_data_extractor._retrieve_stores_data(base_store_endpoint, headers)

            store_data_cleaner = DataCleaning(all_stores_df)
            cleaned_store_data = store_data_cleaner._clean_store_data()

            self.db_connector._upload_to_db(cleaned_store_data, "dim_store_details")

    def extract_and_upload_product_data(self):
        """Extract product data, clean it, and upload it to the database."""
        s3_address = 's3://data-handling-public/products.csv'
        data_extractor = DataExtractor(self.db_engine)

        product_data_df = data_extractor._extract_csv_from_s3(s3_address)

        weights = DataCleaning(product_data_df)
        converted_weights_df = weights._convert_product_weights(product_data_df)

        product_data_cleaner = DataCleaning(converted_weights_df)
        cleaned_product_data = product_data_cleaner._clean_products_data()

        self.db_connector._upload_to_db(cleaned_product_data, 'dim_products')

    def extract_and_upload_orders_data(self):
        """Extract orders data, clean it, and upload it to the database."""
        data_extractor = DataExtractor(self.db_engine)
        order_data = data_extractor._read_rds_table('orders_table')

        order_data_cleaner = DataCleaning(order_data)
        cleaned_orders_data = order_data_cleaner._clean_orders_data()

        self.db_connector._upload_to_db(cleaned_orders_data, 'orders_table')

    def extract_and_upload_date_events_data(self):
        """Extract date events data, clean it, and upload it to the database."""
        date_data_s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        data_extractor = DataExtractor(self.db_engine)

        date_events_data = data_extractor._extract_json_from_s3(date_data_s3_address)

        date_events_data_cleaner = DataCleaning(date_events_data)
        cleaned_date_events_data = date_events_data_cleaner._clean_date_events_data()

        self.db_connector._upload_to_db(cleaned_date_events_data, 'dim_date_times')


if __name__ == "__main__":
    data_handler = DataHandlingMain()

    data_handler.extract_and_upload_user_data()
    data_handler.extract_and_upload_card_data()
    data_handler.extract_and_upload_store_data()
    data_handler.extract_and_upload_product_data()
    data_handler.extract_and_upload_orders_data()
    data_handler.extract_and_upload_date_events_data()

