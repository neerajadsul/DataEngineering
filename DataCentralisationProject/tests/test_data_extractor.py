from pandas import DataFrame

from ..database_utils import DatabaseConnector
from ..data_extractor import DataExtractor


class TestDataExtractor:
    """Unit tests for data extractor module."""

    TEST_TABLE_NAME = 'test_users_table'
    TEST_PDF_FILE = 'tests/test_data_card_details.pdf'
    creds = DatabaseConnector.read_db_creds('db_creds_central.yaml')
    engine = DatabaseConnector.init_db_engine(creds)

    def test_read_rds_table(self):
        test_df = DataExtractor.read_rds_table(self.engine, self.TEST_TABLE_NAME)
        assert isinstance(test_df, DataFrame)

    def test_retrieve_pdf_data(self):
        test_df = DataExtractor.retrieve_pdf_data(self.TEST_PDF_FILE)
        assert isinstance(test_df, DataFrame)
