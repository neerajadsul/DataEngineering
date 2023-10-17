"""Clean data from each of the data sources."""
from pandas import DataFrame


class DataCleaning:
    @staticmethod
    def clean_user_data(user_df: DataFrame, dest_table: str):
        """Clean the user data for NULL values, errors with dates,
        incorrectly typed values and rows filled with the wrong information."""
        print(user_df.head())
