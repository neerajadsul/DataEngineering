"""Extract data from CSV files, a web API or S3 bucket."""
import logging

import pandas as pd
from pandas import DataFrame
import tabula
from sqlalchemy import Engine

logger = logging.getLogger("data_extractor")


class DataExtractor:
    @staticmethod
    def read_rds_table(engine: Engine, src_table: str) -> DataFrame:
        """Read database table into Pandas DataFrame."""
        # Use context manager to avoid dangling open connection
        user_df = pd.read_sql_table(table_name=src_table, con=engine)
        return user_df

    @staticmethod
    def retrieve_pdf_data():
        """Read and extract text data from pdf file from AWS S3 bucket."""
        data_file = r'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        pass

if __name__ == "__main__":
    pass
