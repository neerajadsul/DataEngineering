"""Metrics measurement using predefined analytics queries."""

import glob
import pprint

from sqlalchemy import text

from database_utils import DatabaseConnector


class QueryRunner:

    def __init__(self):
        """Initialize with connectivity to central database."""
        self.db_connector = DatabaseConnector('db_creds_central.yaml')

    def execute_query(self, query_file: str) -> str:
        """Runs SQL query from given file.

        :param query_file: *.sql file path
        """
        with open(query_file, 'r') as f:
            content = f.readlines()

        qt = [x for x in content if not x.startswith('--') and len(x) > 0]
        if len(content) < 1:
            raise ValueError(f'No query found in {query_file}.')

        query_text = text(''.join(qt))

        with self.db_connector.engine.connect() as conn:
            data = conn.execute(query_text)

        return data.all()


def get_report(metrics='all'):
    """Run specified analytics queries and create a markdown report."""
    queries = sorted(glob.glob('queries/*.sql'))

    qr = QueryRunner()

    results = dict()

    for query in queries:
        try:
            data = qr.execute_query(query)
        except Exception as error:
            print(f'Error in {query} \n {error}')
        results[query] = data

    return results


def export_analytics_report(results: dict):
    """Write query results in a markdown file."""

    with open('report_metrics_analytics.md', 'w') as f:
        print('# Analytics Report')
        for rkey, rval in results.items():
            print('## Query: ', rkey, file=f)
            pprint.pprint(rval, stream=f)


if __name__ == "__main__":
    results = get_report()
    export_analytics_report(results)
