from database_utils import DatabaseConnector


class TestDatabaseConnector:

    dbc = DatabaseConnector()

    def test_read_db_creds(self):
        creds = self.dbc.read_db_creds()
        assert creds is not None and isinstance(creds, dict)
        # Five primary connection parameters must be present
        for name in ('HOST', 'PASSWORD', 'USER', 'DATABASE', 'PORT'):
            assert any(key.endswith(name) for key in creds.keys())

    def test_init_db_engine(self):
        creds = self.dbc.read_db_creds()
        engine = self.dbc.init_db_engine(creds)
        assert engine is not None

    def test_list_db_tables(self):
        creds = self.dbc.read_db_creds()
        engine = self.dbc.init_db_engine(creds)        
        table_names = self.dbc.list_db_tables(engine)
        assert len(table_names) > 0
