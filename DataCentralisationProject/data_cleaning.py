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

        # Cleaning up phone numbers: phone_number
        users_df['phone_number'] = self.phone_validation(users_df['phone_number'])

        # log some statistics for investigation when needed
        logger.debug('Number of NaNs: ', users_df.isna().sum())
        logger.debug('Number of Nulls: ', users_df.isnull().sum())

    def email_validation(self, emails: Series) -> Series:
        """
        Replace invalid emails with empty string.
        :param emails: Series
        :return: cleaned-up Series
        """
        email_regex = r"^[^.][a-zA-Z0-9!#$%&'*+-/=?^_`{|}~]{2,64}[@][a-zA-Z0-9.-]{3,}$"
        mask_invalid = ~emails.str.match(email_regex)
        emails.loc[mask_invalid] = str()
        return emails

    def phone_validation(self, phones: Series) -> Series:
        """
        Replace invalid phone numbers with empty string, remove separators.
        :param phones: Series
        :return: cleaned-up Series
        """
        # Remove space, dash, period from phone numbers
        for pattern in (r' ', r'\.', r'\-'):
            phones = phones.str.replace(pattern, '', regex=True)

        # Set invalid phone numbers to nan
        phone_regex = r"^[+]?\d{0,4}(?:\(\d{1,5}\)){0,1}\d{5,12}"
        mask_invalid = ~phones.str.match(phone_regex)
        phones.loc[mask_invalid] = str()
        return phones

