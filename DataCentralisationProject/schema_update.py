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
        query_date_user = text('''ALTER TABLE dim_orders
                                  ALTER COLUMN date_uuid TYPE uuid,
                                  ALTER COLUMN user_uuid TYPE uuid;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_date_user)
        
        query_prod_quantity = text('''ALTER TABLE dim_orders
                                   ALTER COLUMN product_quantity TYPE smallint;''')
        with self.db_connector.engine.connect() as conn:
            conn.execute(query_prod_quantity)

        
