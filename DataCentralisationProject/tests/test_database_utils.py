import pandas as pd
from ..database_utils import DatabaseConnector


class TestDatabaseConnector:
    """Tests for database connector module."""

    creds_file = 'db_creds_central.yaml'

    def test_init_db_engine(self):
        db_connector = DatabaseConnector(self.creds_file)
        assert db_connector.engine is not None

    def test_list_db_tables(self):
        db_connector = DatabaseConnector(self.creds_file)
        table_names = db_connector.list_db_tables()
        assert len(table_names) > 0

    def test_upload_to_db(self):
        TEST_DATA_FILE = 'tests/test_user_data.csv'
        TEST_TABLE_NAME = 'test_users_table'
        # Upload test data for users
        data_write = pd.read_csv(TEST_DATA_FILE)
        db_connector = DatabaseConnector(self.creds_file)
        db_connector.upload_to_db(data_write, TEST_TABLE_NAME)
        # Check whether uploaded and read data is identical
        data_read = pd.read_sql_table(TEST_TABLE_NAME, db_connector.engine)
        # Fail early if number of columns do not match
        assert len(data_write.columns) == len(data_read.columns)
        # Fail early if number of rows do not match
        assert data_write['index'].count() == data_read['index'].count()
        # Finally, compare between written and read data
        assert data_write.equals(data_read)
