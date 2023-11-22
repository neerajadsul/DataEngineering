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
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], errors='coerce')
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], errors='coerce')

        # Check invalid date when after current date
        users_df.loc[users_df['date_of_birth'] > np.datetime64(datetime.datetime.now()), 'date_of_birth'] = np.NaN
        users_df.loc[users_df['join_date'] > np.datetime64(datetime.datetime.now()), 'join_date'] = np.NaN

        # Check for invalid email address: email_address
        users_df['email_address'] = self.email_validation(users_df['email_address'])

        # Cleaning up phone numbers: phone_number
        users_df['phone_number'] = self.phone_validation(users_df['phone_number'])

        # Cleaning up user UUIDs: user_uuid
        user_uuid_regex = r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}'
        invalid_user_uuid = users_df[~users_df['user_uuid'].str.match(user_uuid_regex)].index
        users_df = users_df.drop(invalid_user_uuid)

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

    def clean_card_data(self, df: DataFrame) -> DataFrame:
        """Sanitize Card Data.

        :param df: raw data frame for card transactions
        :return: cleaned card transaction data frame.
        """
        # Drop rows where 4 columns have empty/null value
        df = df.dropna(thresh=4)
        df['card_number'] = df['card_number'].astype('str')
        # Pdf extraction leads to some card numbers prefixed with ?
        df['card_number'] = df['card_number'].str.replace('?', '', regex=False)
        # Validate payment card number or set NaN: card_number
        # Ref: https://en.wikipedia.org/wiki/Payment_card_number
        # Card number can vary from 12 digits to 19 digits
        # most cards can be validated with Luhns Algorithm
        df.loc[~df['card_number'].str.match(r'[0-9]{9,19}'), 'card_number'] = np.NaN
        df = df.dropna(subset='card_number')
        # Validate expiry date or set NaN: expiry_date
        expiry_date_regex = r'[0-9]{2}/[0-9]{2,4}'
        invalid_expiry = ~df['expiry_date'].str.match(expiry_date_regex)
        print(f'{invalid_expiry.sum()} invalid expiry dates found, setting NaN.')
        df.loc[invalid_expiry, 'expiry_date'] = np.NaN
        # Validate confirmed date of payment: date_payment_confirmed
        payment_date_regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
        invalid_payment_dates = ~df['date_payment_confirmed'].str.match(payment_date_regex)
        print(f'{invalid_payment_dates.sum()} invalid payment dates found, setting NaN.')
        df.loc[invalid_payment_dates, 'date_payment_confirmed'] = np.NaN

        return df

    def clean_store_data(self, stores_df: DataFrame) -> DataFrame:
        """Sanitize retail stores data.

        :param store_df: raw retail store data
        :return: cleaned data
        """
        # Remove stores data with invalud staff numbers
        invalid_staff_numbers = stores_df[stores_df['staff_numbers'].apply(lambda x: not x.strip().isnumeric())].index
        stores_df.loc[invalid_staff_numbers, 'staff_numbers'] = 0
        MIN_ADDRESS_LENGTH = 3
        # Remove row without a valid address
        invalid_addresses = stores_df[stores_df['address'].str.len() < MIN_ADDRESS_LENGTH].index
        stores_df.drop(invalid_addresses, inplace=True)
        lat_long_regex = r'-?\d{1,3}.\d{1,}'
        invalid_lat = stores_df[(~stores_df['latitude'].str.match(lat_long_regex, na=False))].index
        stores_df.loc[invalid_lat, 'latitude'] = np.nan
        invalid_lat = stores_df[(~stores_df['lat'].str.match(lat_long_regex, na=False))].index
        stores_df.loc[invalid_lat, 'lat'] = np.nan
        invalid_long = stores_df[(~stores_df['longitude'].str.match(lat_long_regex, na=False))].index
        stores_df.loc[invalid_long, 'longitude'] = np.nan
        stores_df['latitude'] = stores_df['latitude'].fillna(np.NaN)
        stores_df['longitude'] = stores_df['longitude'].fillna(np.NaN)

        stores_df['locality'] = stores_df['locality'].fillna("N/A")
        stores_df = stores_df.dropna(subset=['store_code'])
        stores_df = stores_df.dropna(thresh=4)
        stores_df = stores_df.drop(stores_df[stores_df['store_code'].str.match('NULL')].index)

        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], format='mixed', errors='coerce')
        stores_df['store_type'] = stores_df['store_type'].fillna('')
        stores_df = stores_df.drop(stores_df[stores_df['country_code'].str.len() > 3].index)
        stores_df['continent'] = stores_df['continent'].fillna('')

        return stores_df

    def _convert_product_weights(self, df):
        """Convert all units to kg, assuming 1ml approx equal to 1g.

        :param products_df: dataframe for products information
        :return: dataframe with converted weights to kg
        """
        # Multipack products
        multipack_regex = r'[0-9]{1,}.?([0-9]{1,})?[mlkgoz]+'
        multipack_index = df['weight'][~df['weight'].str.match(multipack_regex)].index
        if len(multipack_index) > 0:
            df.loc[multipack_index, 'weight'] = df.loc[multipack_index, 'weight'].apply(self.__multipack_to_bulk)
        else:
            print('No more multipack products to process')
        # ml to kilograms
        ml_index = df['weight'][df['weight'].str.endswith('ml')].index
        if len(ml_index) > 0:
            df.loc[ml_index, 'weight'] = df.loc[ml_index, 'weight'].apply(self.__ml_to_kg)
        else:
            print('No more liquid products in ml to process')
        # oz to kilograms
        oz_index = df['weight'][df['weight'].str.endswith('oz')].index
        if len(oz_index) > 0:
            df.loc[oz_index, 'weight'] = df.loc[oz_index, 'weight'].apply(self.__oz_to_kg)
        else:
            print('No more products in oz to process')
        # Grams to kilograms
        gram_regex = r'^[0-9]*[.]?[0-9]*[^k][g]'
        gram_index = df['weight'][df['weight'].str.match(gram_regex)].index
        if len(gram_index) > 0:
            df.loc[gram_index, 'weight'] = df.loc[gram_index, 'weight'].apply(self.__grams_to_kg)
        else:
            print('No more products in grams to process')
        # Remove kilogram units
        kg_index = df[df['weight'].apply(lambda x: str(x).endswith('kg'))].index
        if len(kg_index) > 0:
            df.loc[kg_index, 'weight'] = df.loc[kg_index, 'weight'].apply(self.__remove_kg_unit)
        else:
            print('No more products in kg to process')

        return df

    def clean_products_data(self, df) -> DataFrame:
        """Sanitize products data.

        :param products_df: dataframe for products information
        :return: sanitized data for products
        """
        invalid_product_name = df[df['product_name'].isna()].index
        df = df.drop(invalid_product_name)
        price_regex = r'\S{1,3}[0-9]{1,}.[0-9]{2,}'
        df = df.drop(df[~df['product_price'].str.match(price_regex)].index)
        df = df.drop(df[df['product_price'].isna()].index)
        df = df.drop(df[df['weight'].isna()].index)
        df = self._convert_product_weights(df)
        df['category'] = df['category'].fillna('')
        df['EAN'] = df['EAN'].fillna('')
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df['removed'] = df['removed'].fillna('')
        df['product_code'] = df['product_code'].fillna('')

        return df

    @staticmethod
    def __multipack_to_bulk(pack):
        n, amount = pack.split(' x ')
        numeric_amount = ''
        unit_measure = ''
        for c in amount:
            if c.isdigit():
                numeric_amount += c
            if c.isalpha():
                unit_measure += c
                if len(unit_measure) == 2:
                    break
        total = round(float(n) * float(numeric_amount))
        return str(total) + unit_measure

    @staticmethod
    def __grams_to_kg(grams: str):
        amount = grams.split('g')[0]
        return str(round(float(amount)/1000, 3))

    @staticmethod
    def __ml_to_kg(ml: str):
        amount = ml.split('ml')[0]
        return str(round(float(amount)/1000, 3))

    @staticmethod
    def __oz_to_kg(oz: str):
        amount = oz.split('oz')[0]
        return str(round(float(amount)*455/16000, 3))

    @staticmethod
    def __remove_kg_unit(kg: str):
        amount = kg.split('kg')[0]
        return amount

    def clean_orders_data(self, df) -> DataFrame:
        """Sanitize orders data.

        :param df: DataFrame for orders
        :return: cleaned DataFrame
        """
        columns_to_drop = ['1', 'first_name', 'last_name']
        df = df.drop(columns=columns_to_drop)
        return df

    def clean_sales_data(self, df) -> DataFrame:
        """Santize sales data which has following columns
            timestamp, month, year, day, time_period,

        :param df: sales data DataFrame
        :return: clean DataFrame
        """
        invalid_month = df[(~df['month'].str.isnumeric())].index
        df = df.drop(invalid_month)
        invalid_year = df[~df['year'].str.isnumeric()].index
        df = df.drop(invalid_year)
        invalid_day = df[~df['day'].str.isnumeric()].index
        df = df.drop(invalid_day)
        timestamp_regex = r'[0-9]{2}:[0-9]{2}:[0-9]{2}'
        invalid_timestamp = df[~df['timestamp'].str.match(timestamp_regex)].index
        df = df.drop(invalid_timestamp)
        uuid_regex = r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}'
        invalid_date_uuid = df[~df['date_uuid'].str.match(uuid_regex)].index
        df = df.drop(invalid_date_uuid)

        return df
