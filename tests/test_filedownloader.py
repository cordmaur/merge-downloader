"""
Test Utils Module
"""

from pathlib import Path
import pytest
from mergedownloader.file_downloader import FileDownloader, ConnectionType

# from mergedownloader.inpeparser import INPEParsers


class TestFileDownloader:
    """Test the FileDownloader class"""

    @pytest.fixture(scope="session")
    def fixture_data(self):
        """Return the test data for the tests"""
        data = {
            "server": "ftp.cptec.inpe.br",
            "NonExistentFTP": "nonexistent.example.com",
            "RootMergePath": "/modelos/tempo/MERGE/",
            "DownloadTestFile": "READ_ME-MERGE.pdf",
        }
        return data

    def test_initialization(self, fixture_data):
        """
        Test that an HTTP connection is opened and that `is_connected` is True.
        """
        fd = FileDownloader(fixture_data["server"], connection_type=ConnectionType.HTTP)
        assert fd._connection_type == ConnectionType.HTTP  # pylint: disable=W0212
        assert fd.is_connected

        # test for non-existent server
        fd = FileDownloader(
            fixture_data["NonExistentFTP"], connection_type=ConnectionType.HTTP
        )
        assert not fd.is_connected

        # # Test for gaierror when a non-existent server is passed
        # with pytest.raises(gaierror):
        #     FileDownloader(fixture_data["NonExistentFTP"])

    def test_http_download(self, fixture_data):
        """
        Test that a file is downloaded from the HTTP server and that the local file exists.
        """
        fd = FileDownloader(fixture_data["server"], connection_type=ConnectionType.HTTP)
        remote_file = (
            fixture_data["RootMergePath"] + "/" + fixture_data["DownloadTestFile"]
        )
        local_folder = "./tests/data/"
        local_path = fd.download_file(remote_file, local_folder)
        assert isinstance(local_path, Path)
        assert local_path.name == fixture_data["DownloadTestFile"]
        assert local_path.exists()
        local_path.unlink()

