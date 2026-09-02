"""
Test Utils Module
"""

from pathlib import Path
import pytest
from mergedownloader.file_downloader import FileDownloader
from mergedownloader.enums import DownloadMode

# from mergedownloader.inpeparser import INPEParsers


class TestFileDownloader:
    """Test the FileDownloader class"""

    @pytest.fixture(scope="session")
    def fixture_data(self):
        """Return the test data for the tests"""
        data = {
            "server": "https://ftp.cptec.inpe.br",
            "RootMergePath": "/modelos/tempo/MERGE/GPM/DAILY/2026/08/",
            "DownloadTestFile": "MERGE_CPTEC_20260801.grib2",
        }
        return data

    def test_initialization(self):
        """Test that the downloader stores its HTTP download policy."""
        fd = FileDownloader(download_mode=DownloadMode.UPDATE)
        assert fd._download_mode == DownloadMode.UPDATE  # pylint: disable=W0212

    def test_http_download(self, fixture_data):
        """
        Test that a file is downloaded from the HTTP server and that the local file exists.
        """
        fd = FileDownloader()
        remote_file = (
            fixture_data["server"]
            + fixture_data["RootMergePath"]
            + fixture_data["DownloadTestFile"]
        )
        local_folder = "./tests/data/"
        local_path = fd.download_file(remote_file, local_folder)
        assert isinstance(local_path, Path)
        assert local_path.name == fixture_data["DownloadTestFile"]
        assert local_path.exists()
        local_path.unlink()
