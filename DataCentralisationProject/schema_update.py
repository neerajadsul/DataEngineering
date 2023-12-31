from sqlalchemy import text
import logging
import argparse

from database_utils import DatabaseConnector

logger = logging.getLogger(__name__)


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

    def convert_to_date(self, table_name, column_name, casting=False):
        additional = ''
        if casting:
            additional = f'USING {column_name}::date'
        query_to_date = text(f'''ALTER TABLE {table_name}
                            ALTER COLUMN {column_name} TYPE date {additional}''')
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

    def rename_column(self, table_name, column_name, new_name):
        query_rename = text(f'''ALTER TABLE {table_name}
                                RENAME COLUMN {column_name} TO {new_name};''')
        with self.engine.connect() as conn:
            conn.execute(query_rename)
            conn.commit()

    def set_column_nullable(self, table_name, column_name):
        query_nullable = text(f'''ALTER TABLE {table_name}
                            ALTER COLUMN {column_name} DROP NOT NULL;''')
        with self.engine.connect() as conn:
            conn.execute(query_nullable)
            conn.commit()

    def set_primary_key(self, table_name, column_name):
        query_pk = text(f'''ALTER TABLE {table_name}
                            ADD PRIMARY KEY ({column_name});''')
        with self.engine.connect() as conn:
            conn.execute(query_pk)
            conn.commit()

    def add_foreign_key_constraint(self, central_table, central_column, dim_table, dim_column):
        """Add foreign key constraint to central column refering to dimensions table primary key.

        :param central_table: _description_
        :param central_column: _description_
        :param dim_table: _description_
        :param dim_column: _description_
        """
        query_fk_pk = text(f'''ALTER TABLE {central_table}
                            ADD CONSTRAINT fk_{central_column}
                            FOREIGN KEY ({central_column})
                            REFERENCES {dim_table}({dim_column});''')

        with self.engine.connect() as conn:
            conn.execute(query_fk_pk)
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
    TABLE_NAME = 'orders_table'
    logger.debug(f'Altering {TABLE_NAME}')
    # Convert date and user columns to UUIDs
    dsu._convert_to_uuid(TABLE_NAME, 'date_uuid')
    dsu._convert_to_uuid(TABLE_NAME, 'user_uuid')
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
    logger.debug(f'Altering {TABLE_NAME}')
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
    logger.debug(f'Altering {TABLE_NAME}')
    dsu._convert_to_float(TABLE_NAME, 'longitude')
    # Merge latitude and lat into latitude, drop lat
    with dsu.engine.connect() as conn:
        conn.execute(text(f'UPDATE {TABLE_NAME} SET latitude = CAST(CONCAT(lat, latitude, 0) AS DOUBLE PRECISION);'))
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


def alter_products_table(dsu: DbSchemaUpdater):
    """Update products table as per the specifications below.
    +-----------------+--------------------+--------------------+
    |  dim_products   | current data type  | required data type |
    +-----------------+--------------------+--------------------+
    | product_price   | TEXT               | FLOAT              |
    | weight          | TEXT               | FLOAT              |
    | EAN             | TEXT               | VARCHAR(?)         |
    | product_code    | TEXT               | VARCHAR(?)         |
    | date_added      | TEXT               | DATE               |
    | uuid            | TEXT               | UUID               |
    | removed         | TEXT               | BOOL               |
    | weight_class    | TEXT               | VARCHAR(?)         |
    +-----------------+--------------------+--------------------+

    :param dsu: DbSchemaUpdater
    """
    # Remove £ from product_price column
    with dsu.engine.connect() as conn:
        conn.execute(text("UPDATE dim_products SET product_price = TRIM('£' FROM product_price);"))
        conn.commit()
    TABLE_NAME = 'dim_products'
    logger.debug(f'Altering {TABLE_NAME}')
    # Convert weight to float
    dsu._convert_to_float(TABLE_NAME, 'weight')
    # Add new column `weight_class`
    with dsu.engine.connect() as conn:
        conn.execute(text(f'ALTER TABLE {TABLE_NAME} ADD COLUMN weight_class TEXT;'))
        conn.execute(text(f'''UPDATE {TABLE_NAME}
            SET weight_class = CASE
                    WHEN weight < 2 THEN 'Light'
                    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                    WHEN weight >=40 AND weight < 140 THEN 'Heavy'
                    WHEN weight >= 140 THEN 'Truck_Required'
                END
        '''))
        conn.commit()
    dsu._convert_to_float(TABLE_NAME, 'product_price')
    dsu._convert_to_varchar(TABLE_NAME, '"EAN"', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'product_code', max_data_length=True)
    dsu.convert_to_date(TABLE_NAME, 'date_added')
    dsu._convert_to_uuid(TABLE_NAME, 'uuid')
    # Rename column to `still_available`
    dsu.rename_column(TABLE_NAME, 'removed', 'still_available')
    # Change column type to bool
    conditions = {True: "LIKE 'Still_ava%'", False: "LIKE 'Remove%'"}
    dsu.convert_to_boolean(TABLE_NAME, 'still_available', conditions)
    dsu._convert_to_varchar(TABLE_NAME, 'weight_class', max_data_length=True)


def alter_sales_date_times_table(dsu: DbSchemaUpdater):
    """Update sales data table containing dates and times as per specifications below
    +-----------------+-------------------+--------------------+
    | dim_date_times  | current data type | required data type |
    +-----------------+-------------------+--------------------+
    | month           | TEXT              | VARCHAR(?)         |
    | year            | TEXT              | VARCHAR(?)         |
    | day             | TEXT              | VARCHAR(?)         |
    | time_period     | TEXT              | VARCHAR(?)         |
    | date_uuid       | TEXT              | UUID               |
    +-----------------+-------------------+--------------------+

    :param dsu: DbSchemaUpdater
    """
    TABLE_NAME = 'dim_date_times'
    logger.debug(f'Altering {TABLE_NAME}')
    dsu._convert_to_varchar(TABLE_NAME, 'month', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'year', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'day', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'time_period', max_data_length=True)
    dsu._convert_to_uuid(TABLE_NAME, 'date_uuid')


def alter_card_details_table(dsu: DbSchemaUpdater):
    """Update card details table as per the specifications below:
    +------------------------+-------------------+--------------------+
    |    dim_card_details    | current data type | required data type |
    +------------------------+-------------------+--------------------+
    | card_number            | TEXT              | VARCHAR(?)         |
    | expiry_date            | TEXT              | VARCHAR(?)         |
    | date_payment_confirmed | TEXT              | DATE               |
    +------------------------+-------------------+--------------------+

    :param dsu: DbSchemaUpdater
    """
    TABLE_NAME = 'dim_card_details'
    logger.debug(f'Altering {TABLE_NAME}')
    dsu._convert_to_varchar(TABLE_NAME, 'card_number', max_data_length=True)
    dsu._convert_to_varchar(TABLE_NAME, 'expiry_date', max_data_length=True)
    dsu.convert_to_date(TABLE_NAME, 'date_payment_confirmed', casting=True)


def set_primary_keys_dim_tables(dsu: DbSchemaUpdater):
    """Primary keys setup to have orders table as single source of truth.
    Primary key     Corresponding Table
    -----------------------------------
    date_uuid       dim_date_times
    user_uuid       dim_users
    store_code      dim_stores_data
    product_code    dim_products
    card_number     dim_card_details

    :param dsu: DbSchemaUpdater
    """
    logger.debug('Setting Primary Keys in Dimensions Table')
    dsu.set_primary_key('dim_date_times', 'date_uuid')
    dsu.set_primary_key('dim_users', 'user_uuid')
    dsu.set_primary_key('dim_stores_data', 'store_code')
    dsu.set_primary_key('dim_products', 'product_code')
    dsu.set_primary_key('dim_card_details', 'card_number')


def set_foreign_keys_orders_table(dsu: DbSchemaUpdater):
    """Set up specified foreign keys from following tables
    Foreign key     Corresponding Table
    -----------------------------------
    date_uuid       dim_date_times
    user_uuid       dim_users
    store_code      dim_stores_data
    product_code    dim_products
    card_number     dim_card_details

    :param dsu: _description_
    """
    CENTRAL_TABLE = 'orders_table'
    logger.debug(f'Setting Foreign Keys (FK) in Central Table `{CENTRAL_TABLE}`')
    dsu.add_foreign_key_constraint(CENTRAL_TABLE, 'date_uuid', 'dim_date_times', 'date_uuid')
    dsu.add_foreign_key_constraint(CENTRAL_TABLE, 'user_uuid', 'dim_users', 'user_uuid')
    dsu.add_foreign_key_constraint(CENTRAL_TABLE, 'store_code', 'dim_stores_data', 'store_code')
    dsu.add_foreign_key_constraint(CENTRAL_TABLE, 'product_code', 'dim_products', 'product_code')
    dsu.add_foreign_key_constraint(CENTRAL_TABLE, 'card_number', 'dim_card_details', 'card_number')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='''Choose steps to execute and logging 
                                                    level during star-schema creation.''')

    parser.add_argument('-a', '--alter', action='store_true', help='Alter dimensions table schemas.')
    parser.add_argument('-pk', '--set-pk', action='store_true', help='Set primary keys on dimensions tables.')
    parser.add_argument('-fk', '--set-fk', action='store_true', help='Set foreign key constraint on central table.')
    parser.add_argument('-d', '--debug-log', action='store_true')

    args = parser.parse_args()

    if not any((args.alter, args.set_pk, args.set_fk)):
        parser.print_help()
        exit(0)

    logger.setLevel(logging.DEBUG)

    dsu = DbSchemaUpdater('db_creds_central.yaml')
    if args.alter:
        alter_orders_table(dsu)
        alter_users_table(dsu)
        alter_stores_table(dsu)
        alter_products_table(dsu)
        alter_sales_date_times_table(dsu)
        alter_card_details_table(dsu)
    if args.set_pk:
        set_primary_keys_dim_tables(dsu)
    if args.set_fk:
        set_foreign_keys_orders_table(dsu)
