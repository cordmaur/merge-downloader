"""
Tests for the INPEDownloader classes
"""
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

from mergedownloader.downloader import Downloader
from mergedownloader.inpeparser import INPEParsers, INPETypes
from mergedownloader.utils import GISUtil


class TestDownloader:
    """Docstring"""

    @pytest.fixture(scope="session")
    def fixture_data(self):
        """Return the test data for the tests"""
        data = {
            "test_date": "2023-03-01",
            "end_date": "2023-03-03",
            "correct_structure": "2023/03",
            "correct_filename": "MERGE_CPTEC_20230301.grib2",
            "NonExistentFTP": "nonexistent.example.com",
            "DownloadTestFile": "gera_Normais.ksh",
        }
        return data

    downloader = Downloader(
        server=INPEParsers.FTPurl,
        parsers=INPEParsers.parsers,
        local_folder="./tests/data",
    )

    @patch("mergedownloader.downloader.FTPUtil")
    def test_init(self, _):
        """Test Downloader initialization"""
        # create the instance
        downloader = Downloader(
            server="ftp.example.com",
            parsers=INPEParsers.parsers,
            local_folder="tests/data",
            avoid_update=True,
        )

        # assert instance variables
        assert isinstance(downloader.ftp, MagicMock)
        assert isinstance(downloader.parsers, list)
        assert isinstance(downloader.local_folder, Path)
        assert isinstance(downloader.avoid_update, bool)

        # assert parsers initialization
        for parser in downloader.parsers:
            assert parser.ftp == downloader.ftp
            assert parser.avoid_update == downloader.avoid_update
            assert parser.datatype in downloader.data_types

    def test_cut_cube_by_geoms(self):
        """Test Downloader cut_cube_by_geoms method"""
        # create mocks
        mock_cube = MagicMock()
        mock_geoms = MagicMock()

        # call the method
        GISUtil.cut_cube_by_geoms(mock_cube, mock_geoms)

        # assert calls were done correctly
        assert mock_geoms.to_crs.called
        assert mock_cube.rio.clip.called

    def test_get_parser(self):
        """Test the get_parser function"""
        for parser in self.downloader.parsers:
            assert parser == self.downloader.get_parser(parser.datatype)

    def test_get_time_series(self):
        """Test Downloader get_time_series method"""
        # create mocks
        mock_cube = MagicMock()
        mock_shp = MagicMock()
        mock_reducer = MagicMock()

        mock_cube.dims = ["time", "longitude"]

        # call the method
        Downloader.get_time_series(mock_cube, mock_shp, mock_reducer)

        # assert calls
        assert mock_cube.rio.clip.called
        assert mock_reducer.called

    def test_is_downloaded(self, fixture_data):
        """Test if a specific date is already downloaded and updated"""

        test_dates = [fixture_data["test_date"], fixture_data["end_date"], "20230302"]
        results = [True, True, False]
        for date, result in zip(test_dates, results):
            is_down = self.downloader.is_downloaded(
                date_str=date,
                datatype=INPETypes.DAILY_RAIN,
            )
            assert is_down == result

    # @patch("raindownloader.utils.FTPUtil.download_ftp_file")
    # def test_download_file(self, mock_download_ftp, fixture_data):
    #     """Test the download file method with a mock to avoid actual download"""

    #     f = Path("")
    #     mock_download_ftp.return_value = f

    #     downloaded_file = self.downloader.download_file(
    #         date_str=fixture_data["test_date"],
    #         datatype=INPETypes.DAILY_RAIN,
    #     )

    #     # assert download called and Path type was not modified
    #     assert mock_download_ftp.called
    #     assert downloaded_file == f
