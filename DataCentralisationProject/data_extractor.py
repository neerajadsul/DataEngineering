"""Extract data from CSV files, a web API or S3 bucket."""
import logging
import requests
import json

import pandas as pd
from pandas import DataFrame
import tabula
from sqlalchemy import Engine
import boto3

logger = logging.getLogger('data_extractor')


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
            return dfs[0]

    @staticmethod
    def list_number_of_stores(header_details, endpoint):
        """Retrieve number of stores from given API endpoint."""
        data = requests.get(endpoint, headers=header_details)
        if data.status_code != 200:
            raise Exception(f'Request failed: HTTP code {data.status_code}')
        else:
            data = data.content.decode()
            data = json.loads(data)
            return data['number_stores']

    @staticmethod
    def retrieve_stores_data(total_n_stores, header_details, endpoint, stop_n=None) -> DataFrame:
        """Retrieves data for all stores with given endpoint, supports loading subset of stores.

        :param total_n_stores: total number of stores
        :param header_details: API token key
        :param endpoint: endpoint url
        :param stop_n: only load these many stores, defaults to all stores
        :return: stores DataFrame
        """
        if stop_n is None:
            stop_n = total_n_stores

        stores_data = []
        errors = ['Data retrieval failed for stores: ']
        for store_number in range(int(stop_n)):
            # Create a store specific url endpoint
            store_endpoint = f'{endpoint}/{store_number}'
            # Retrieve data for a store number
            data = requests.get(store_endpoint, headers=header_details)
            if data.status_code != 200:
                errors.append(store_number)
                continue
            data = data.content.decode()
            data = json.loads(data)
            stores_data.append(data)

        stores_df = pd.json_normalize(stores_data)

        return stores_df

    @staticmethod
    def extract_from_s3(resource_uri) -> DataFrame:
        bucket = 'data-handling-public'
        file_name = 'products.csv'
        s3 = boto3.client('s3')

        file_object = s3.get_object(Bucket=bucket, Key=file_name)
        df = pd.read_csv(file_object['Body'])

        return df


if __name__ == '__main__':
    pass
