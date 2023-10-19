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
        logger.debug(actual_outcome)
        logger.debug(expected_outcome)
        assert actual_outcome.equals(expected_outcome)

