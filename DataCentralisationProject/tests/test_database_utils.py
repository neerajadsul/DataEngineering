from database_utils import DatabaseConnector


class TestDatabaseConnector:

    dbc = DatabaseConnector()

    def test_read_db_creds(self):
        creds = self.dbc.read_db_creds()
        assert creds is not None and isinstance(creds, dict)
        # Five primary connection parameters must be present
        for name in ('HOST', 'PASSWORD', 'USER', 'DATABASE', 'PORT'):
            assert any(key.endswith(name) for key in creds.keys())

