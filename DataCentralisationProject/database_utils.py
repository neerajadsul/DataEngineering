"""Connect and upload data to the specified databased."""
from sqlalchemy import create_engine, Engine
from sqlalchemy import inspect
import yaml
from pandas import DataFrame


class DatabaseConnector:
    """Methods to interact with a database with given credentials."""

    def __init__(self, creds_file):
        try:
            with open(creds_file) as f:
                creds = yaml.load(f, yaml.Loader)
        except FileNotFoundError:
            print(f'File not found, {creds_file}')
        except IOError:
            print(f'Found but could not access {creds_file}')
        self.engine = self.__init_db_engine(creds=creds)

    @staticmethod
    def __init_db_engine(creds, db_type='postgresql', db_api='psycopg2') -> Engine:
        db_host = creds['RDS_HOST']
        db_user = creds['RDS_USER']
        db_pass = creds['RDS_PASSWORD']
        db_name = creds['RDS_DATABASE']
        db_port = creds['RDS_PORT']
        engine = create_engine(f'{db_type}+{db_api}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
        return engine

    def list_db_tables(self) -> list:
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names

    def upload_to_db(self, df: DataFrame, dest_table: str) -> None:
        """Store Pandas DataFrame to database with given table name."""
        df.to_sql(dest_table, self.engine, if_exists='replace', index=False)


if __name__ == '__main__':
    dbc = DatabaseConnector(creds_file='db_creds_central.yaml')
