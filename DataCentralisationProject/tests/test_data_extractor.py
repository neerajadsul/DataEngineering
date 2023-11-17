from pathlib import Path
from pandas import DataFrame

from database_utils import DatabaseConnector
from data_extractor import DataExtractor


class TestDataExtractor:
    """Unit tests for data extractor module."""

    TEST_TABLE_NAME = 'test_users_table'
    TEST_PDF_FILE = 'tests/test_data_card_details.pdf'
    TEST_CREDS_FILE = Path(__file__).parents[1].resolve() / 'db_creds_central.yaml'
    db_connector = DatabaseConnector(TEST_CREDS_FILE)

    def test_read_rds_table(self):
        test_df = DataExtractor.read_rds_table(self.db_connector, self.TEST_TABLE_NAME)
        assert isinstance(test_df, DataFrame)

    def test_retrieve_pdf_data(self):
        test_df = DataExtractor.retrieve_pdf_data(self.TEST_PDF_FILE)
        assert isinstance(test_df, DataFrame)

    def test_extract_from_s3(self):
        s3_resource_uri = r's3://data-handling-public/products.csv'
        actual = DataExtractor.extract_from_s3(s3_resource_uri)
        assert isinstance(actual, DataFrame)

    def test_get_bucket_key_file(self):
        s3_resource_uri = 's3://www.sample.com/courses/inner/125.pdf'
        bucket, file_key = DataExtractor._get_bucket_key_file(s3_resource_uri)
        assert bucket == 'www.sample.com'
        assert file_key == 'courses/inner/125.pdf'
