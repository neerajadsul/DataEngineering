"""Data centralisation workflow."""
from database_utils import DatabaseConnector
from data_extractor import DataExtractor
from data_cleaning import DataCleaning


def process_users_data():
    """Gather data from multiple sources and upload to the central database."""
    # Customer's data retrieval
    db_connector = DatabaseConnector()
    creds = db_connector.read_db_creds('db_creds_aws_rds.yaml')
    engine = db_connector.init_db_engine()
    table_names: list = db_connector.list_db_tables()
    users_table = [name for name in table_names if 'users' in name]
    assert len(users_table) == 1
    users_table = users_table[0]
    # - Read users raw data from AWS RDS database
    data_extractor = DataExtractor()
    user_df = data_extractor.read_rds_table(db_connector, users_table)

    # Customer's Data Cleaning
    data_cleaner = DataCleaning()
    # - Clean raw users data before uploading to central database
    clean_user_df = data_cleaner.clean_user_data(user_df, 'dim_users')


def process_user_card_data():
    pass


if __name__ == "__main__":
    process_users_data()
