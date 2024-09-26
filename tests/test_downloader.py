"""
Tests for the INPEDownloader classes
"""

import os
import shutil
from datetime import timedelta
from pathlib import Path

# from unittest.mock import patch, MagicMock
import pytest

import xarray as xr

from mergedownloader.downloader import Downloader
from mergedownloader.file_downloader import ConnectionType, FileDownloader
from mergedownloader.parser import AbstractParser
from mergedownloader.inpeparser import InpeParsers, InpeTypes, INPE_SERVER, DailyParser
from mergedownloader.utils import DateProcessor, DateFrequency

# from mergedownloader.utils import GISUtil


class TestDownloader:
    """Docstring"""

    @pytest.fixture(scope="session")
    def fixture_data(self):
        """Return the test data for the tests"""
        data = {
            "test_dir": "./tests/data",
            "test_date": "2023-03-01",
            "end_date": "2023-03-05",
            "correct_structure": "2023/03",
            "correct_filename": "MERGE_CPTEC_20230301.grib2",
            "NonExistentFTP": "nonexistent.example.com",
            "DownloadTestFile": "gera_Normais.ksh",
        }
        return data

    @pytest.fixture(scope="session")
    def downloader(self, fixture_data):
        """Setup the downloader instance for all the tests"""

        print("Cleaning up data folder")
        # perform a `rm -r *` on the test_dir

        if os.path.isdir(fixture_data["test_dir"]):
            shutil.rmtree(fixture_data["test_dir"])

        # create the folder
        os.mkdir(fixture_data["test_dir"])

        print("Setting up resources")

        fd = FileDownloader(server=INPE_SERVER, connection_type=ConnectionType.HTTP)
        downloader = Downloader(
            file_downloader=fd,
            parsers=InpeParsers,
            local_folder=fixture_data["test_dir"],
        )
        yield downloader

        print("Releasing resources")

    def test_init(self, downloader):
        """Test if self.downloader is initialized correctly"""
        print("testing initialization")

        assert downloader is not None

        # pylint: disable=W0212
        assert isinstance(downloader._file_downloader, FileDownloader)
        assert isinstance(downloader._parsers, dict)
        assert isinstance(downloader._local_folder, Path)
        # pylint: enable=W0212

    def test_get_parser(self, downloader):
        """Test the get_parser method"""

        print("testing get_parser method")
        parser1 = downloader.get_parser(InpeTypes.DAILY_RAIN)
        parser2 = downloader.get_parser("DAILY_RAIN")
        assert parser1 == InpeParsers[InpeTypes.DAILY_RAIN]
        assert parser2 == InpeParsers[InpeTypes.DAILY_RAIN]
        assert isinstance(parser1, AbstractParser)
        assert isinstance(parser2, DailyParser)

    def test_get_file(self, downloader, fixture_data):
        """Test the get_file for all a parser and a processor"""
        print("testing get_file method")

        # get test data from fixture_data
        date_str = fixture_data["test_date"]

        # get a valid date
        target = downloader.get_file(date_str, InpeTypes.DAILY_RAIN)
        assert isinstance(target, Path)
        assert target.exists()

        # get a non-existent date
        target = downloader.get_file("1900-01-01", InpeTypes.DAILY_RAIN)
        assert target is None

    def test_get_files(self, downloader, fixture_data):
        """ "Test getting multiple files"""

        # get test data from fixture_data
        date_str = fixture_data["test_date"]
        end_date_str = fixture_data["end_date"]

        dates = DateProcessor.dates_range(date_str, end_date_str, DateFrequency.DAILY)

        # get a valid date
        response = downloader.get_files(dates, InpeTypes.DAILY_RAIN)
        assert isinstance(response, list)
        assert len(response) == len(dates)

    def test_get_range(self, downloader, fixture_data):
        """ "Test getting a range of dates"""

        # get test data from fixture_data
        start_date = DateProcessor.parse_date(fixture_data["end_date"])
        start_date = start_date + timedelta(days=1)
        end_date = start_date + timedelta(days=5)

        # get a valid date
        response = downloader.get_range(start_date, end_date, InpeTypes.DAILY_RAIN)
        assert isinstance(response, list)
        assert len(response) == 6

    def test_create_cube_dates(self, downloader, fixture_data):
        """Test the create_cube_dates method"""

        print("testing create_cube_dates method")
        # get test data from fixture_data
        # get test data from fixture_data
        start_date = DateProcessor.parse_date(fixture_data["end_date"])
        start_date = start_date + timedelta(days=6)
        end_date = start_date + timedelta(days=5)

        dates = DateProcessor.dates_range(start_date, end_date, DateFrequency.DAILY)

        print(f"dates: {dates}")

        cube = downloader.create_cube_dates(dates, InpeTypes.DAILY_RAIN)
        assert isinstance(cube, xr.DataArray)
        assert len(cube.time) == len(dates)

        assert set(cube.dims) == {"time", "longitude", "latitude"}
        assert cube.rio.crs is not None

    def test_open_file(self, downloader, fixture_data):
        """Test the open_file for a .grib and a .nc files"""

        # get test data from fixture_data
        date_str = fixture_data["test_date"]

        print("Testing opening a .grib2 file")
        arr = downloader.open_file(date_str, InpeTypes.DAILY_RAIN)
        assert isinstance(arr, xr.DataArray)
        assert arr.rio.crs is not None
        assert set(arr.dims) == {"latitude", "longitude"}

        print("Testing opening a .nc file")
        file = downloader.get_file(date_str, InpeTypes.MONTHLY_ACCUM_MANUAL)
        dset = xr.open_dataset(file)
        assert isinstance(dset, xr.Dataset)
        assert dset.rio.crs is not None
        assert set(dset.dims) == {"time", "latitude", "longitude"}
        assert set(dset.attrs).issuperset({"days", "last_day", "updated"})

    # downloader = Downloader(
    #     server=INPEParsers.FTPurl,
    #     parsers=INPEParsers.parsers,
    #     local_folder="./tests/data",
    # )

    # @patch("mergedownloader.downloader.FTPUtil")
    # def test_init(self, _):
    #     """Test Downloader initialization"""
    #     # create the instance
    #     downloader = Downloader(
    #         server="ftp.example.com",
    #         parsers=INPEParsers.parsers,
    #         local_folder="tests/data",
    #         avoid_update=True,
    #     )

    #     # assert instance variables
    #     assert isinstance(downloader.ftp, MagicMock)
    #     assert isinstance(downloader.parsers, list)
    #     assert isinstance(downloader.local_folder, Path)
    #     assert isinstance(downloader.avoid_update, bool)

    #     # assert parsers initialization
    #     for parser in downloader.parsers:
    #         assert parser.ftp == downloader.ftp
    #         assert parser.avoid_update == downloader.avoid_update
    #         assert parser.datatype in downloader.data_types

    # def test_cut_cube_by_geoms(self):
    #     """Test Downloader cut_cube_by_geoms method"""
    #     # create mocks
    #     mock_cube = MagicMock()
    #     mock_geoms = MagicMock()

    #     # call the method
    #     GISUtil.cut_cube_by_geoms(mock_cube, mock_geoms)

    #     # assert calls were done correctly
    #     assert mock_geoms.to_crs.called
    #     assert mock_cube.rio.clip.called

    # def test_get_parser(self):
    #     """Test the get_parser function"""
    #     for parser in self.downloader.parsers:
    #         assert parser == self.downloader.get_parser(parser.datatype)

    # def test_get_time_series(self):
    #     """Test Downloader get_time_series method"""
    #     # create mocks
    #     mock_cube = MagicMock()
    #     mock_shp = MagicMock()
    #     mock_reducer = MagicMock()

    #     mock_cube.dims = ["time", "longitude"]

    #     # call the method
    #     Downloader.get_time_series(mock_cube, mock_shp, mock_reducer)

    #     # assert calls
    #     assert mock_cube.rio.clip.called
    #     assert mock_reducer.called

    # def test_is_downloaded(self, fixture_data):
    #     """Test if a specific date is already downloaded and updated"""

    #     test_dates = [fixture_data["test_date"], fixture_data["end_date"], "20230302"]
    #     results = [True, True, False]
    #     for date, result in zip(test_dates, results):
    #         is_down = self.downloader.is_downloaded(
    #             date_str=date,
    #             datatype=INPETypes.DAILY_RAIN,
    #         )
    #         assert is_down == result

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
