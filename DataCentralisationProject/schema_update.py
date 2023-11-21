from sqlalchemy import text

from database_utils import DatabaseConnector


class DbSchemaUpdater:
    """Modify central database schema for analytics."""
    def __init__(self, creds_file) -> None:
        self.db_connector = DatabaseConnector(creds_file)
        self.engine = self.db_connector.engine

    def _convert_to_varchar(self, table_name, column_name, max_data_length=True):
        if isinstance(max_data_length, bool) and max_data_length:
            query_max_length = text(f'SELECT MAX(LENGTH({column_name})) FROM {table_name}')
            with self.engine.connect() as conn:
                conn.execute(query_max_length)
                max_data_length = conn.execute(text(f'SELECT MAX(LENGTH({column_name})) FROM {table_name}'))
            max_data_length = max_data_length.scalar_one()
        elif not isinstance(max_data_length, int) or max_data_length < 1:
            raise ValueError(f'Varchar(?) length should be integer > 0, got {max_data_length}.')

        query_modify = text(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({max_data_length})')
        with self.engine.connect() as conn:
            conn.execute(query_modify)
            conn.commit()

    def _convert_to_smallint(self, table_name, column_name):
        query_smallint = text(f'''ALTER TABLE {table_name}
                                   ALTER COLUMN {column_name} TYPE smallint USING {column_name}::smallint;''')
        with self.engine.connect() as conn:
            conn.execute(query_smallint)
            conn.commit()

    def _convert_to_float(self, table_name, column_name):
        query_float = text(f'''ALTER TABLE {table_name}
                                   ALTER COLUMN {column_name} TYPE float USING {column_name}::double precision;''')
        with self.engine.connect() as conn:
            conn.execute(query_float)
            conn.commit()

    def _convert_to_text(self, table_name, column_name):
        query_to_text = text(f'''ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE TEXT;''')
        with self.engine.connect() as conn:
            conn.execute(query_to_text)
            conn.commit()

    def _convert_to_uuid(self, table_name, column_name):
        query_uuid = text(f'''ALTER TABLE {table_name}
                                  ALTER COLUMN {column_name} TYPE uuid USING {column_name}::uuid;''')
        with self.engine.connect() as conn:
            conn.execute(query_uuid)
            conn.commit()

    def convert_to_date(self, table_name, column_name):
        query_to_date = text(f'''ALTER TABLE {table_name}
                            ALTER COLUMN {column_name} TYPE date''')
        with self.engine.connect() as conn:
            conn.execute(query_to_date)
            conn.commit()

    def convert_to_boolean(self, table_name, column_name, conditions):
        query_bool = text(f'''ALTER TABLE {table_name}
            ALTER COLUMN {column_name} TYPE bool
            USING CASE
                WHEN {column_name} {conditions[True]} THEN true
                WHEN {column_name} {conditions[False]} THEN false
                ELSE false
                END
        ''')
        with self.engine.connect() as conn:
            conn.execute(query_bool)
            conn.commit()

    def set_column_nullable(self, table_name, column_name):
        query_nullable = text(f'''ALTER TABLE {table_name}
                            ALTER COLUMN {column_name} DROP NOT NULL;''')
        with self.engine.connect() as conn:
            conn.execute(query_nullable)
            conn.commit()


def alter_orders_table(dsu: DbSchemaUpdater):
    """Update tables schema as per the specifications below:
    +------------------+--------------------+--------------------+
    |   orders_table   | current data type  | required data type |
    +------------------+--------------------+--------------------+
    | date_uuid        | TEXT               | UUID               |
    | user_uuid        | TEXT               | UUID               |
    | card_number      | TEXT               | VARCHAR(?)         |
    | store_code       | TEXT               | VARCHAR(?)         |
    | product_code     | TEXT               | VARCHAR(?)         |
    | product_quantity | BIGINT             | SMALLINT           |
    +------------------+--------------------+--------------------+

    :param dsu: DbSchemaUpdater
    """
    TABLE_NAME = 'dim_orders'
    # Convert date and user columns to UUIDs
    dsu._convert_to_uuid(TABLE_NAME, 'date_uuid')
    # Convert product quantity to SMALLINT from BIGINT
    dsu._convert_to_smallint(TABLE_NAME, 'product_quantity')
    # Convert card_number to TEXT
    dsu._convert_to_text(TABLE_NAME, 'card_number')
    # Covert card_number to VARCHAR with maximum length for card number.
    dsu._convert_to_varchar(TABLE_NAME, 'card_number', max_data_length=True)
    # Convert store_code and product_code to VARCHAR with maximum length for the respective code
    dsu._convert_to_varchar(TABLE_NAME, 'store_code', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'product_code', max_data_length=True)


def alter_users_table(dsu: DbSchemaUpdater):
    """Update tables schema as per the specifications below:
    +----------------+--------------------+--------------------+
    | dim_user_table | current data type  | required data type |
    +----------------+--------------------+--------------------+
    | first_name     | TEXT               | VARCHAR(255)       |
    | last_name      | TEXT               | VARCHAR(255)       |
    | date_of_birth  | TEXT               | DATE               |
    | country_code   | TEXT               | VARCHAR(?)         |
    | user_uuid      | TEXT               | UUID               |
    | join_date      | TEXT               | DATE               |
    +----------------+--------------------+--------------------+

    :param dsu: DbSchemaUpdater
    """
    TABLE_NAME = 'dim_users'
    dsu._convert_to_varchar(TABLE_NAME, 'first_name', 255)
    dsu._convert_to_varchar(TABLE_NAME, 'last_name', 255)
    dsu.convert_to_date(TABLE_NAME, 'date_of_birth')
    dsu._convert_to_varchar(TABLE_NAME, 'country_code', max_data_length=True)
    dsu._convert_to_uuid(TABLE_NAME, 'user_uuid')
    dsu.convert_to_date(TABLE_NAME, 'join_date')


def alter_stores_table(dsu: DbSchemaUpdater):
    """Update stores detail table schema as per the specifications below.
    +---------------------+-------------------+------------------------+
    | store_details_table | current data type |   required data type   |
    +---------------------+-------------------+------------------------+
    | longitude           | TEXT              | FLOAT                  |
    | locality            | TEXT              | VARCHAR(255)           |
    | store_code          | TEXT              | VARCHAR(?)             |
    | staff_numbers       | TEXT              | SMALLINT               |
    | opening_date        | TEXT              | DATE                   |
    | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
    | latitude            | TEXT              | FLOAT                  |
    | country_code        | TEXT              | VARCHAR(?)             |
    | continent           | TEXT              | VARCHAR(255)           |
    +---------------------+-------------------+------------------------+

    :param dsu: DbSchemaUpdater
    """
    TABLE_NAME = 'dim_stores_data'
    dsu._convert_to_float(TABLE_NAME, 'longitude')
    # Merge latitude and lat into latitude
    with dsu.engine.connect() as conn:
        conn.execute(text(f'UPDATE {TABLE_NAME} SET latitude = CAST(CONCAT(lat, latitude) AS DOUBLE PRECISION);'))
        conn.execute(text(f'ALTER TABLE {TABLE_NAME} DROP COLUMN lat'))
        conn.commit()

    dsu._convert_to_varchar(TABLE_NAME, 'locality', max_data_length=255)
    dsu._convert_to_varchar(TABLE_NAME, 'store_code', max_data_length=True)
    dsu._convert_to_smallint(TABLE_NAME, 'staff_numbers')
    dsu.convert_to_date(TABLE_NAME, 'opening_date')
    dsu._convert_to_varchar(TABLE_NAME, 'store_type', max_data_length=255)
    dsu.set_column_nullable(TABLE_NAME, 'store_type')
    dsu._convert_to_varchar(TABLE_NAME, 'country_code', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'continent', max_data_length=255)


if __name__ == "__main__":
    dsu = DbSchemaUpdater('db_creds_central.yaml')
    # alter_orders_table(dsu)
    # alter_users_table(dsu)
    alter_stores_table(dsu)
