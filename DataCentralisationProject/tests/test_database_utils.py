import pandas as pd
from ..database_utils import DatabaseConnector


class TestDatabaseConnector:
    """Tests for database connector module."""
    
    dbc = DatabaseConnector()

    def test_read_db_creds(self):
        creds = self.dbc.read_db_creds('db_creds_aws_rds.yaml')
        assert creds is not None and isinstance(creds, dict)
        # Five primary connection parameters must be present
        for name in ('HOST', 'PASSWORD', 'USER', 'DATABASE', 'PORT'):
            assert any(key.endswith(name) for key in creds.keys())

    def test_init_db_engine(self):
        creds = self.dbc.read_db_creds('db_creds_central.yaml')
        engine = self.dbc.init_db_engine(creds)
        assert engine is not None

    def test_list_db_tables(self):
        creds = self.dbc.read_db_creds('db_creds_aws_rds.yaml')
        engine = self.dbc.init_db_engine(creds)
        table_names = self.dbc.list_db_tables(engine)
        assert len(table_names) > 0

    def test_upload_to_db(self):
        TEST_DATA_FILE = 'tests/test_user_data.csv'
        TEST_TABLE_NAME = 'test_users_table'
        # Upload test data for users
        data_write = pd.read_csv(TEST_DATA_FILE)        
        self.dbc.upload_to_db(data_write, TEST_TABLE_NAME)
        # Check whether uploaded and read data is identical
        creds = self.dbc.read_db_creds('db_creds_central.yaml')
        engine = self.dbc.init_db_engine(creds)
        data_read = pd.read_sql_table(TEST_TABLE_NAME, engine)
        # Fail early if number of columns do not match
        assert len(data_write.columns) == len(data_read.columns)
        # Fail early if number of rows do not match
        assert data_write['index'].count() == data_read['index'].count()
        # Finally, compare between written and read data
        assert data_write.equals(data_read)

        