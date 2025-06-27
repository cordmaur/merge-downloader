"""Test the DateProcessor class"""
from datetime import datetime

from mergedownloader.utils import DateFrequency, DateProcessor


class TestDateProcessor:
    """Class to test the DateProcessor class"""

    @staticmethod
    def test_parse_date():
        """Test the parse_date method"""
        # Test parsing of datetime object
        date = datetime.now()
        assert DateProcessor.parse_date(date) == date

        # Test parsing of string with ISO format
        date_str = "2022-04-14T12:00:00"
        expected_date = datetime(2022, 4, 14, 12, 0, 0)
        assert DateProcessor.parse_date(date_str) == expected_date

        # Test parsing of string with custom format
        date_str = "2022/04/14"
        expected_date = datetime(2022, 4, 14)
        assert DateProcessor.parse_date(date_str) == expected_date

    @staticmethod
    def test_normalize_date():
        """Test the normalize_date method"""
        # Test normalization of datetime object
        date = datetime(2022, 4, 14)
        assert DateProcessor.normalize_date(date) == "20220414"

        # Test normalization of string with ISO format
        date_str = "2022-04-14T12:00:00"
        assert DateProcessor.normalize_date(date_str) == "20220414"

        # Test normalization of string with custom format
        date_str = "2022/04/14"
        assert DateProcessor.normalize_date(date_str) == "20220414"

    @staticmethod
    def test_pretty_date():
        """Test the pretty_date method"""
        # Test pretty printing of datetime object
        date = datetime(2022, 4, 14)
        assert DateProcessor.pretty_date(date) == "14-04-2022"

        # Test pretty printing of string with ISO format
        date_str = "2022-04-14T12:00:00"
        assert DateProcessor.pretty_date(date_str) == "14-04-2022"

        # Test pretty printing of string with custom format
        date_str = "2022/04/14"
        assert DateProcessor.pretty_date(date_str) == "14-04-2022"

    @staticmethod
    def test_dates_range():
        """Test the dates_range method"""
        # Test daily frequency
        start_date = datetime(2022, 4, 1)
        end_date = datetime(2022, 4, 3)
        expected_dates = ["20220401", "20220402", "20220403"]
        assert (
            DateProcessor.dates_range(start_date, end_date, DateFrequency.DAILY)
            == expected_dates
        )

        # Test monthly frequency
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2022, 4, 1)
        expected_dates = ["20220101", "20220201", "20220301", "20220401"]
        assert (
            DateProcessor.dates_range(start_date, end_date, DateFrequency.MONTHLY)
            == expected_dates
        )

        # Test yearly frequency
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2022, 1, 1)
        expected_dates = ["20200101", "20210101", "20220101"]
        assert (
            DateProcessor.dates_range(start_date, end_date, DateFrequency.YEARLY)
            == expected_dates
        )

    # Test month_abrev method
    def test_month_abrev(self):
        """Test month_abrev method"""
        date_str = "2021-01-01"
        month_abrev = DateProcessor.month_abrev(date_str)
        assert month_abrev == "jan"

    # Test start_end_dates method
    def test_start_end_dates(self):
        """Test start_end_dates method"""
        date_str = "2021-01-01"
        start_end_dates = DateProcessor.start_end_dates(date_str)
        assert start_end_dates == ("20210101", "20210131")

    # Test last_n_months method
    def test_last_n_months(self):
        """Test last_n_months method"""
        date_str = "2021-06-01"
        lookback = 6
        last_n_months = DateProcessor.last_n_months(date_str, lookback)
        assert last_n_months == ("2021-1", "2021-6")

    # Test create_monthly_periods method
    def test_create_monthly_periods(self):
        """Test create_monthly_periods method"""
        start_date = "2021-01-01"
        end_date = "2021-12-01"
        month_step = 3
        monthly_periods = DateProcessor.create_monthly_periods(
            start_date, end_date, month_step
        )
        expected_periods = [
            (datetime(2021, 1, 1), datetime(2021, 3, 1)),
            (datetime(2021, 4, 1), datetime(2021, 6, 1)),
            (datetime(2021, 7, 1), datetime(2021, 9, 1)),
            (datetime(2021, 10, 1), datetime(2021, 12, 1)),
        ]
        assert monthly_periods == expected_periods
