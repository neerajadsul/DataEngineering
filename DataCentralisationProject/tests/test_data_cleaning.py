"""Tests for DataCleaning class"""
import logging
import pandas as pd

from ..data_cleaning import DataCleaning

logger = logging.getLogger()

class TestDataCleaning():

    data_cleaner = DataCleaning()

    def test_email_validation(self):
        test_emails_validity = {
            "john.doe1234@gmail.com": True,
            "sarah.smith5678@hotmail.com": True,
            "mike_jones99@yahoo.com": True,
            "emilybrown123@outlook.com": True,
            "chris.green12@icloud.com": True,
            "jessica.parker34@gmail.com": True,
            "david.miller56@yahoo.com": True,
            "amanda_jones789@hotmail.com": True,
            "mark_taylor23@outlook.com": True,
            "lisa_smith45@icloud.com": True,
            "invalid.email@.com": False,
            "another.invalid.email@.com": False,
            "test.email@.com": False,
            "not.valid.email@.com": False,
            "fake.email@@.com": False,
            "nvalid.email.com": False,
            "missingatsymbolcom": False,
            "user@missingdotcom": False,
            "@missingusername.com": False
        }
        emails = pd.Series(test_emails_validity.keys(), name='email_address')
        expected_outcome = pd.Series([k if v else str() for k, v in test_emails_validity.items()], name='email_address')
        actual_outcome = self.data_cleaner.email_validation(emails)
        # logger.debug(actual_outcome)
        # logger.debug(expected_outcome)
        assert actual_outcome.equals(expected_outcome)


    def test_phone_validation(self):
        test_phone_validity = {
            "+1 555-555-5555": True,
            "+44 20 1234 5678": True,
            "+61 2 1234 5678": True,
            "+81 3 1234 5678": True,
            "(+49) 30 12345678": True,
            "+33 1 23 45 67 89": True,
            "+55 11 1234-5678": True,
            "+91 11 1234 5678": True,
            "+86 10 1234 5678": True,
            "+7 495 123-45-67": True,
            "+1 555-555-5555x123": True,
            "(+44 20 1234 567": False,
            "+61) 2 1234": False,
            "+81 3 1234 5678 ext 456": False,
            "+49 30he 12345": False,
            "+33-YELLOW-456-456": False,
            "0000+0011 1234-56789": False,
            "IND 11 123 4567": False,
            "?86 10 1234": False,
            "+7 495# 123-45-6": False
        }
        phones = pd.Series(test_phone_validity.keys(), name='phone_number')
        expected_outcome = pd.Series([k if v else str() for k, v in test_phone_validity.items()], name='phone_number')
        # logger.debug(expected_outcome)
        actual_outcome = self.data_cleaner.phone_validation(phones)
        # logger.debug(actual_outcome)
        assert actual_outcome.isna().sum() == expected_outcome.isna().sum()