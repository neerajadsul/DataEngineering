"""Clean data from each of the data sources."""
import logging
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import datetime

logger = logging.getLogger('data_cleaning')


class DataCleaning:
    """Data cleaning for dates, emails, empty values, phone numbers."""
    def clean_user_data(self, users_df: DataFrame):
        """Clean the user data for NULL values, errors with dates,
        incorrectly typed values and rows filled with the wrong information."""
        # Check for duplicates
        users_df.drop_duplicates(inplace=True)

        # Cleaning date columns: date_of_birth and join_date
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], infer_datetime_format=True, errors='coerce')

        # Check invalid date when after current date
        users_df.loc[users_df['date_of_birth'] > np.datetime64(datetime.datetime.now()), 'date_of_birth'] = np.NaN
        users_df.loc[users_df['join_date'] > np.datetime64(datetime.datetime.now()), 'join_date'] = np.NaN

        # Check for invalid email address: email_address
        users_df['email_address'] = self.email_validation(users_df['email_address'])

        # log some statistics for investigation when needed
        logger.debug('Number of NaNs: ', users_df.isna().sum())
        logger.debug('Number of Nulls: ', users_df.isnull().sum())

    def email_validation(self, emails: Series) -> Series:
        """
        Replace invalid emails with empty string.
        :param emails: Series
        :return: cleaned-up Series
        """
        # Ref: https://uibakery.io/regex-library/email-regex-python
        email_regex = "^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
        mask_invalid = ~emails.str.match(email_regex)
        emails.loc[mask_invalid] = str()
        return email


