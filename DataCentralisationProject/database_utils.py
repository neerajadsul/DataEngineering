"""Connect and upload data to the specified databased."""
from sqlalchemy import create_engine, Engine
from sqlalchemy import inspect
import yaml
from pandas import DataFrame


class DatabaseConnector:
    @staticmethod
    def read_db_creds(creds_file='db_creds.yaml'):
        creds = None
        try:
            with open(creds_file) as f:
                creds = yaml.load(f, yaml.Loader)
        except FileNotFoundError:
            print(f'File not found, {creds_file}')
        except IOError:
            print(f'Found but could not access {creds_file}')
        return creds

    @staticmethod
    def init_db_engine(creds) -> Engine:
        db_type = 'postgresql'
        db_api = 'psycopg2'
        db_host = creds['RDS_HOST']
        db_user = creds['RDS_USER']
        db_pass = creds['RDS_PASSWORD']
        db_name = creds['RDS_DATABASE']
        db_port = creds['RDS_PORT']
        engine = create_engine(
            f'{db_type}+{db_api}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
        )
        return engine

    @staticmethod
    def list_db_tables(engine: Engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names

    @staticmethod
    def upload_to_db(user_df: DataFrame, dest_table: str):
        """Store Pandas DataFrame to database with given table name."""
        creds = DatabaseConnector.read_db_creds(creds_file='db_creds_central.yaml')
        engine = DatabaseConnector.init_db_engine(creds)
        user_df.to_sql(dest_table, engine, if_exists='replace', index=False)


if __name__ == '__main__':
    pass
