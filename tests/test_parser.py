"""Test the BaseParser class"""
import os
from pathlib import Path
from unittest.mock import MagicMock
from mergedownloader.parser import BaseParser, DateFrequency


class TestBaseParser:
    """Test the Base Parser"""

    # @pytest.fixture(autouse=True)
    def setup_method(self):
        """Setup Function"""
        # Instantiate the BaseParser object with some arguments
        self.base_parser = BaseParser(  # pylint: disable=attribute-defined-outside-init
            datatype="test_data",
            root="test_root",
            filename_fn=lambda dt: f"test_file_{dt.strftime('%Y-%m-%d')}.nc",
            foldername_fn=lambda dt: f"test_folder/{dt.strftime('%Y/%m')}/",
            date_freq=DateFrequency.DAILY,
            ftp=MagicMock(),
            avoid_update=True,
            post_proc=None,
        )
        self.temp_folder = (  # pylint: disable=attribute-defined-outside-init
            "./tests/data"
        )

    # Test the filename() method
    def test_filename(self):
        """Docstring"""
        assert self.base_parser.filename("2022-01-01") == "test_file_2022-01-01.nc"

    # Test the local_path() method
    def test_local_path(self):
        """Docstring"""
        local_path = self.base_parser.local_path(self.temp_folder)
        assert os.path.isdir(local_path)
        assert local_path == Path("./tests/data/test_data")

    # Test the local_target() method
    def test_local_target(self):
        """Docstring"""
        local_target = self.base_parser.local_target("2022-01-01", self.temp_folder)
        assert local_target == Path("./tests/data/test_data/test_file_2022-01-01.nc")

    # Test the remote_path() method
    def test_remote_path(self):
        """ "Docstring"""
        remote_path = self.base_parser.remote_path("2022-01-01")
        assert remote_path == "test_root/test_folder/2022/01/"

    # Test the remote_target() method
    def test_remote_target(self):
        """Docstring"""
        assert (
            self.base_parser.remote_target("2022-01-01")
            == "test_root/test_folder/2022/01/test_file_2022-01-01.nc"
        )

    # Test the dates_range() method
    def test_dates_range(self):
        """Docstring"""

        dates = self.base_parser.dates_range("2022-01-01", "2022-01-03")
        assert dates == ["20220101", "20220102", "20220103"]

    # Test the download_file() method
    def test_download_file(self):
        """Docstring"""

        # get local and remote targets
        local_target = self.base_parser.local_target("2022-01-01", self.temp_folder)
        remote_target = self.base_parser.remote_target("2022-01-01")

        # download the file through ftp mocked class
        self.base_parser.download_file("2022-01-01", self.temp_folder)

        # assert the mock function was called correctly
        # the called function should be download_ftp_file
        mock_fn = self.base_parser.ftp.download_ftp_file

        mock_fn.assert_any_call(
            remote_file=remote_target,
            local_folder=local_target.parent,
        )
