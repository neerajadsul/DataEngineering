"""Extract data from CSV files, a web API or S3 bucket."""
import pandas as pd
from pandas import DataFrame
import tabula

from database_utils import DatabaseConnector


class DataExtractor:
    @staticmethod
    def read_rds_table(engine, src_table: str) -> DataFrame:
        """Read database table into Pandas DataFrame."""
        pass

    @staticmethod
    def retrieve_pdf_data():
        """Read and extract text data from pdf file from AWS S3 bucket."""
        data_file = r'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        pass

if __name__ == "__main__":
    pass
