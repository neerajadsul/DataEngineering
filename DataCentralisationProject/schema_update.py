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
        # Convert date and user columns to UUIDs
        query_date_user = text('''ALTER TABLE dim_orders
                                  ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
                                  ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_date_user)
            conn.commit()

        # Convert product quantity to SMALLINT from BIGINT
        query_prod_quantity = text('''ALTER TABLE dim_orders
                                   ALTER COLUMN product_quantity TYPE smallint;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_prod_quantity)
            conn.commit()

        # Convert card_number to TEXT and then to VARCHAR with maximum length of the string.
        query_card_number = text('''ALTER TABLE dim_orders ALTER COLUMN card_number TYPE TEXT;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_card_number)
            conn.commit()
        self._convert_to_varchar_max_length('card_number')

        self._convert_to_varchar_max_length('store_code')
        self._convert_to_varchar_max_length('product_code')

    def _convert_to_varchar_max_length(self, column_name):
        # Convert store_code to VARCHAR with maximum length of the string.
        query_max_length = text(f'SELECT MAX(LENGTH({column_name})) FROM dim_orders')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_max_length)
            max_length = conn.execute(text(f'SELECT MAX(LENGTH({column_name})) FROM dim_orders'))
        max_length = max_length.scalar_one()

        query_modify = text(f'ALTER TABLE dim_orders ALTER COLUMN {column_name} TYPE VARCHAR({max_length})')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_modify)
            conn.commit()


if __name__ == "__main__":
    dsu = DbSchemaUpdate('db_creds_central.yaml')
    dsu.alter_orders_table()