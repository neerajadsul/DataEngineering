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
    def retrieve_pdf_data(pdf_file: str, pages='all') -> DataFrame:
        """Read and extract text data as DataFrame from
        a pdf file stored in AWS S3 bucket."""
        try:
            dfs = tabula.read_pdf(pdf_file, pages=pages)
        except FileNotFoundError:
            logger.error(f'File is not accessible at the link: {pdf_file}')
        except ValueError:
            logger.error(f'Invalid file format: {pdf_file}')
        except Exception as e:
            logger.error(e)
        else:
            cards_df = dfs[0]

        return cards_df


if __name__ == "__main__":
    pass
