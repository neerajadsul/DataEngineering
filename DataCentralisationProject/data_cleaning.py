"""Clean data from each of the data sources."""
import logging
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import datetime

logger = logging.getLogger('data_cleaning')


class DataCleaning:
    """Data cleaning for dates, emails, empty values, phone numbers."""
    def clean_user_data(self, users_df: DataFrame) -> DataFrame:
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

        return users_df

    def email_validation(self, emails: Series) -> Series:
        """
        Replace invalid emails with empty string.
        :param emails: Series
        :return: cleaned-up Series
        """
        email_regex = r"^[^.][a-zA-Z0-9!#$%&'*+-/=?^_`{|}~]{2,64}[^.]?[@][a-zA-Z0-9-]{3,32}(?:[.][a-zA-Z0-9-]{2,4})+"
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
        # Regex:
        # - can start with + or upto 4 digits or (country/area code) format
        # - followed by 5 to 12 digits
        # - optionally an extension indicated by an inline literal x, 1 to 6 digits

        # 0.24% rejection rate, allows +10#### type numbers.
        # phone_regex = r"^[+|(]?[0-9]{0,4}(?:\(\d{1,5}\)){0,1}[0-9]{5,12}(?:x\d{1,6})?$"
        # 0.34% rejection rate, safer and cleaner
        phone_regex = r"^(?:\(\+?\d{1,7}\))?(?:\+[1-9][^0])?(?:\(0\))?(?:0{1,2})?(?:\d{5,14})?[x]?(?:\d{1,5})$"
        mask_invalid = ~phones.str.match(phone_regex)
        phones.loc[mask_invalid] = str()
        return phones

    def clean_card_data(self, card_df: DataFrame) -> DataFrame:
        """_summary_

        :param card_df: raw data frame for card transactions
        :return: cleaned card transaction data frame.
        """
        # Validate payment card number or set NaN: card_number
        # Ref: https://en.wikipedia.org/wiki/Payment_card_number
        # Card number can vary from 12 digits to 19 digits
        # most cards can be validated with Luhns Algorithm
        card_df['card_number'] = card_df['card_number'].astype('str')
        invalid_cards = card_df['card_number'].apply(lambda x: 12 > len(x) > 20)
        print(f'{invalid_cards.sum()} invalid card numbers found, dropping those rows.')
        card_df['card_number'][invalid_cards] = np.NaN
        # Validate expiry date or set NaN: expiry_date
        expiry_date_regex = r'[0-9]{2}/[0-9]{2,4}'
        invalid_expiry = ~card_df['expiry_date'].str.match(expiry_date_regex)
        print(f'{invalid_expiry.sum()} invalid expiry dates found, dropping those rows.')
        card_df['expiry_date'][invalid_expiry] = np.NaN
        # Validate confirmed date of payment: date_payment_confirmed
        payment_date_regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
        invalid_payment_dates = ~card_df['date_payment_confirmed'].str.match(payment_date_regex)
        print(f'{invalid_payment_dates.sum()} invalid payment dates found, dropping those rows.')
        card_df['date_payment_confirmed'][invalid_payment_dates] = np.NaN

        return card_df.dropna()

    def clean_store_data(self, stores_df: DataFrame) -> DataFrame:
        """Sanitize retail stores data.

        :param store_df: raw retail store data
        :return: cleaned data
        """
        MIN_ADDRESS_LENGTH = 10
        # Remove row without a valid address 
        invalid_addresses = stores_df[stores_df['address'].str.len() < MIN_ADDRESS_LENGTH].index
        stores_df.drop(invalid_addresses, inplace=True)
        stores_df['longitude'] = stores_df['latitude'].fillna(np.NaN)
        stores_df['longitude'] = stores_df['longitude'].fillna(np.NaN)
        stores_df['locality'] = stores_df['locality'].fillna("")
        stores_df['store_code'].fillna('')
        invalid_staff_numbers = stores_df[stores_df['staff_numbers'].apply(lambda x: not x.isnumeric())].index
        stores_df.drop(invalid_staff_numbers, inplace=True)
        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], format='mixed')
        stores_df['store_type'] = stores_df['store_type'].fillna('')
        stores_df['country_code'] = stores_df['country_code'].fillna('')
        stores_df['continent'] = stores_df['continent'].fillna('')

        return stores_df
