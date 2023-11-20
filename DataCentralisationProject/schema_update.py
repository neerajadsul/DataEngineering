from sqlalchemy import text

from database_utils import DatabaseConnector


class DbSchemaUpdate:
    """Modify central database schema for analytics."""
    def __init__(self, creds_file) -> None:
        self.db_connector = DatabaseConnector(creds_file)

    def alter_orders_table(self):
        """
        Orders Table Schema
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
        """
        TABLE_NAME = 'dim_orders'
        # Convert date and user columns to UUIDs
        self._convert_to_uuid(TABLE_NAME, 'date_uuid')
        # Convert product quantity to SMALLINT from BIGINT
        self._convert_to_smallint(TABLE_NAME, 'product_quantity')
        # Convert card_number to TEXT 
        self._convert_to_text(TABLE_NAME, 'card_number')
        # Covert card_number to VARCHAR with maximum length for card number.
        self._convert_to_varchar(TABLE_NAME, 'card_number', max_data_length=True)
        # Convert store_code and product_code to VARCHAR with maximum length for the respective code
        self._convert_to_varchar('store_code', max_data_length=True)
        self._convert_to_varchar('product_code', max_data_length=True)

    def _convert_to_varchar(self, table_name, column_name, max_data_length=True):
        if isinstance(max_data_length, bool) and max_data_length:
            query_max_length = text(f'SELECT MAX(LENGTH({column_name})) FROM dim_orders')
            with self.db_connector.engine.connect() as conn:
                conn.execute(query_max_length)
                max_length = conn.execute(text(f'SELECT MAX(LENGTH({column_name})) FROM dim_orders'))
            max_length = max_length.scalar_one()
        elif not isinstance(max_data_length, int) or max_data_length < 1:
            raise ValueError(f'Varchar(?) length should be integer > 0, got {max_data_length}.')

        query_modify = text(f'ALTER TABLE dim_orders ALTER COLUMN {column_name} TYPE VARCHAR({max_length})')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_modify)
            conn.commit()

    def _convert_to_smallint(self, table_name, column_name):
        query_smallint = text(f'''ALTER TABLE {table_name}
                                   ALTER COLUMN {column_name} TYPE smallint;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_smallint)
            conn.commit()

    def _convert_to_text(self, table_name, column_name):
        query_to_text = text(f'''ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE TEXT;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_to_text)
            conn.commit()

    def _convert_to_uuid(self, table_name, column_name):
        query_uuid = text(f'''ALTER TABLE {table_name}
                                  ALTER COLUMN {column_name} TYPE uuid USING {column_name}::uuid;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_uuid)
            conn.commit()


if __name__ == "__main__":
    dsu = DbSchemaUpdate('db_creds_central.yaml')
    dsu.alter_orders_table()