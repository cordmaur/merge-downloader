"""
Tests
"""
from pathlib import Path
from datetime import datetime
from socket import gaierror
import ftplib
import pytest
from raindownloader.utils import FTPUtil, OSUtil
from raindownloader.inpeparser import INPEParsers


class TestFTPUtil:
    """Test the FTPUtil class"""

    @pytest.fixture(scope="session")
    def fixture_data(self):
        """Return the test data for the tests"""
        data = {
            "FTPurl": INPEParsers.FTPurl,
            "NonExistentFTP": "nonexistent.example.com",
            "DailyMERGEPath": INPEParsers.daily_rain_parser.root,
            "DownloadTestFile": "gera_Normais.ksh",
        }
        return data

    def test_ftp_connection(self, fixture_data):
        """
        Test that an FTP connection is opened and that `is_connected` is True.
        """
        ftp = FTPUtil(fixture_data["FTPurl"])
        assert isinstance(ftp.ftp, ftplib.FTP)
        assert ftp.is_connected

        # Test for gaierror when a non-existent server is passed
        with pytest.raises(gaierror):
            FTPUtil(fixture_data["NonExistentFTP"])

    def test_ftp_download(self, fixture_data):
        """
        Test that a file is downloaded from the FTP server and that the local file exists.
        """
        ftp = FTPUtil(fixture_data["FTPurl"])
        remote_file = (
            fixture_data["DailyMERGEPath"] + "/" + fixture_data["DownloadTestFile"]
        )
        local_folder = "./tests/data/"
        local_path = ftp.download_ftp_file(remote_file, local_folder)
        assert isinstance(local_path, Path)
        assert local_path.name == fixture_data["DownloadTestFile"]
        assert local_path.exists()
        local_path.unlink()

    def test_ftp_file_info(self, fixture_data):
        """
        Test that the modification time and size of a file on the FTP server are returned.
        """
        ftp = FTPUtil(fixture_data["FTPurl"])
        remote_file = (
            fixture_data["DailyMERGEPath"] + "/" + fixture_data["DownloadTestFile"]
        )
        file_info = ftp.get_ftp_file_info(remote_file)
        assert isinstance(file_info, dict)
        assert isinstance(file_info["datetime"], datetime)
        assert isinstance(file_info["size"], int)
        assert len(file_info) == 2


class TestOSUtil:
    """Test the OSUTil class"""

    def test_get_local_file_info(self):
        """Test the get_local_file_info method"""
        file_info = OSUtil.get_local_file_info(
            "./tests/data/DAILY_RAIN/MERGE_CPTEC_20230301.tif"
        )

        assert isinstance(file_info["datetime"], datetime)
        assert isinstance(file_info["size"], int)
