"""Connect and upload data to the specified databased."""
from sqlalchemy import create_engine, Engine
import yaml


class DatabaseConnector:
    def read_db_creds(self, creds_file='db_creds.yaml'):
        creds = None
        try:
            with open(creds_file) as f:
                creds = yaml.load(f, yaml.Loader)
        except FileNotFoundError:
            print(f'File not found, {creds_file}')
        except IOError:
            print(f"Found but could not access {creds_file}")
        return creds

    def init_db_engine(self, creds) -> Engine:
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


if __name__ == "__main__":
    pass
