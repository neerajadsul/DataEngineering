from pathlib import Path
import random

import yaml
import sqlalchemy
from sqlalchemy import text


root_dir = Path(__file__).resolve().parents[0]


class AWSDBConnector:
    def __init__(self):
        with open(root_dir / 'db_creds_pinterest.yaml') as f:
            creds = yaml.safe_load(f)
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']

        self.engine = self.__create_db_connector()

    def __create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f'mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4'
        )
        return engine

    def random_post(self) -> dict:
        """Returns a random post data containing
            - pin 
            - geolocation
            - user
        """

        random_row = random.randint(0, 11000)
        with self.engine.connect() as connection:
            pin_string = text(f'SELECT * FROM pinterest_data LIMIT {random_row}, 1')
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f'SELECT * FROM geolocation_data LIMIT {random_row}, 1')
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f'SELECT * FROM user_data LIMIT {random_row}, 1')
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping) 

        return {'pin': pin_result, 'geo': geo_result, 'user': user_result}
