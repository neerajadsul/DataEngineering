"""Data centralisation workflow."""
from database_utils import DatabaseConnector
from data_extractor import DataExtractor
from data_cleaning import DataCleaning


def process_users_data():
    """Gather data from multiple sources and upload to the central database."""
    # Customer's data retrieval
    creds = DatabaseConnector.read_db_creds("db_creds_aws_rds.yaml")
    engine = DatabaseConnector.init_db_engine(creds)
    table_names = DatabaseConnector.list_db_tables(engine)
    users_table = [name for name in table_names if "users" in name]
    assert len(users_table) == 1
    users_table = users_table[0]
    # - Read users raw data from AWS RDS database
    users_df = DataExtractor.read_rds_table(engine, users_table)
    # Customer's Data Cleaning
    data_cleaner = DataCleaning()
    # - Clean raw users data before uploading to central database
    clean_user_df = data_cleaner.clean_user_data(users_df=users_df)
    # - Upload cleaned data to central database
    DatabaseConnector.upload_to_db(clean_user_df, 'dim_users')


def process_user_card_data():
    pdf_file = (
        r"https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    )
    card_df = DataExtractor.retrieve_pdf_data(pdf_file)
    data_cleaner = DataCleaning()
    clean_card_df = data_cleaner.clean_card_data(card_df)
    # Upload cards data to central database
    DatabaseConnector.upload_to_db(clean_card_df, 'dim_card_details')


def process_store_data():
    header_details = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    endpoint = r'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_of_stores = DataExtractor.list_number_of_stores(header_details, endpoint)

    endpoint = r'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    stores_df = DataExtractor.retrieve_stores_data(
        number_of_stores, header_details, endpoint
        )
    data_cleaner = DataCleaning()
    clean_stores_df = data_cleaner.clean_store_data(stores_df)
    DatabaseConnector.upload_to_db(clean_stores_df, 'dim_stores_data')


def process_product_details():
    resource_uri = r's3://data-handling-public/products.csv'
    products_df = DataExtractor.extract_from_s3(resource_uri)
    data_cleaner = DataCleaning()
    clean_products_df = data_cleaner.clean_products_data(products_df)
    DatabaseConnector.upload_to_db(clean_products_df, 'dim_products')


def process_orders_data():
    # Orders's data retrieval
    creds = DatabaseConnector.read_db_creds("db_creds_aws_rds.yaml")
    engine = DatabaseConnector.init_db_engine(creds)
    table_names = DatabaseConnector.list_db_tables(engine)
    table = [name for name in table_names if "orders" in name]
    assert len(table) == 1
    # - Read users raw data from AWS RDS database
    df = DataExtractor.read_rds_table(engine, table[0])
    # Orders's Data Cleaning
    # - Clean raw orders data before uploading to central database
    data_cleaner = DataCleaning()
    clean_df = data_cleaner.clean_orders_data(df)
    # - Upload cleaned data to central database
    DatabaseConnector.upload_to_db(clean_df, 'dim_orders')


if __name__ == "__main__":
    # process_users_data()
    # process_user_card_data()
    # process_store_data()
    # process_product_details()
    process_orders_data()
