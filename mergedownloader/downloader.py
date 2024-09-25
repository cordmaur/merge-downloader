"""
Module with specialized classes to understand the INPE FTP Structure
"""

from pathlib import Path
from enum import Enum
from typing import Union, List, Optional, Iterable, Dict
from datetime import datetime
import logging
from logging import handlers

import xarray as xr

from .parser import AbstractParser, AbstractProcessor
from .file_downloader import FileDownloader
from .utils import DateProcessor

# from .utils import FTPUtil, OSUtil, DateProcessor
# from .enums import DataTypes


class Downloader:
    """
    Business logic to download, open and combine the files from a given structure.
    """

    def __init__(
        self,
        file_downloader: FileDownloader,
        parsers: Dict[Enum, AbstractParser],
        local_folder: Union[str, Path],
        log_level: int = logging.INFO,
    ):
        # store initialization variables
        self._file_downloader = file_downloader
        self._parsers = parsers
        self._local_folder = Path(local_folder)

        # self.avoid_update = avoid_update

        self._logger = self.init_logger(log_level)
        self._logger.info("Initializing the Downloader class")

    # -------------------- Logger Functions --------------------
    def init_logger(self, log_level: int):
        """Initialize the loggers (downloader and parsers)"""

        # create the logger
        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)

        # if the logger doesn't have any handlers, setup everything
        if logger.hasHandlers():
            logger.handlers.clear()

        handler = Downloader.create_logger_handler(self._local_folder, log_level)
        logger.addHandler(handler)

        # setup Downloader logger
        if self._file_downloader.logger.hasHandlers():
            self._file_downloader.logger.handlers.clear()
        self._file_downloader.logger.addHandler(handler)
        self._file_downloader.logger.setLevel(log_level)

        # setup parser loggers
        for parser in self._parsers.values():
            if parser.logger.hasHandlers():
                parser.logger.handlers.clear()
            parser.logger.addHandler(handler)
            parser.logger.setLevel(log_level)

        return logger

    @staticmethod
    def create_logger_handler(folder: Union[str, Path], level: int):
        """
        Create a logger file handler for all the project
        :param folder: Folder to put the log gile
        :return: file handler
        """
        # set the base level as DEBUG
        logging.basicConfig(level=logging.DEBUG)

        # clear the handler of the root logger to avoid messages being sent to console
        logging.getLogger().handlers.clear()

        path = Path(folder)
        file_handler = handlers.RotatingFileHandler(
            path / "downloader.log", maxBytes=1024 * 1024, backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s:%(levelname)s:%(name)s] %(message)s",
                datefmt="%Y%m%d-%H%M%S",
            )
        )
        return file_handler

    # -------------------- Private Functions --------------------
    def _process_file(self, date: datetime, processor: AbstractProcessor, **kwargs):
        """todo: write docstring"""

        local_target = processor.local_target(
            date=date, output=self._local_folder, **kwargs
        )

        # First, check with the processor if the file has to be created/updated
        if not processor.must_update(
            date=date, output_folder=self._local_folder, **kwargs
        ):
            return local_target

        # If the file does not exists locally, let's start the processing
        # Initially, let's get list of dependencies (if there are any)
        dependencies = processor.inform_dependencies(date=date, **kwargs)

        # get the dependencies
        if dependencies is not None:
            filled_dependencies = {}
            for dtype, dates in dependencies.items():
                files = []
                for d in dates:
                    arr = self.open_file(date=d, datatype=dtype, **kwargs)

                    if arr is not None:
                        files.append(arr)

                filled_dependencies[dtype] = files

        else:
            filled_dependencies = None

        # Now, let's process the file (it returns as a dataset)
        dset = processor.create_file(
            date=date, dependencies=filled_dependencies, **kwargs
        )

        # Then, let's create the file
        self._logger.info("Creating file %s", local_target)
        dset.to_netcdf(local_target)

        return local_target

    def _download_file(self, date: datetime, parser: AbstractParser, **kwargs):
        """todo: write docstring"""

        local_folder = parser.local_folder(
            date=date, output_folder=self._local_folder, **kwargs
        )

        remote_target = parser.remote_target(date=date, **kwargs)

        # download the file
        return self._file_downloader.download_file(
            remote_file=remote_target, local_folder=local_folder
        )

    # -------------------- Utilities Functions --------------------
    def get_parser(self, datatype: Union[Enum, str]) -> AbstractParser:
        """
        Get the parser associated with the given datatype
        """
        if isinstance(datatype, Enum):
            if datatype in self._parsers:
                return self._parsers[datatype]

        if isinstance(datatype, str):
            for dtype in self._parsers.keys():
                if dtype.name == datatype:
                    return self._parsers[dtype]

        raise ValueError(f"Parser not found for data type {datatype}")

    # -------------------- Download Functions --------------------
    def get_file(
        self, date: Union[str, datetime], datatype: Union[Enum, str], **kwargs
    ) -> Path:
        """
        Get the desired file, given a date and a datatype. The `FileDownloader` will be responsible
        for deciding whether the file should be downloaded or not.
        """

        # get the parser and the date in datetime format
        date = DateProcessor.parse_date(date)
        parser = self.get_parser(datatype=datatype)

        self._logger.info("Getting file %s for %s", datatype, date)

        # If the parser is not a processor, just grab the target location and download the file
        if not isinstance(parser, AbstractProcessor):
            return self._download_file(date, parser, **kwargs)

        # Otherwise, it its a "Processor" let's call the process_file function
        else:
            return self._process_file(date=date, processor=parser, **kwargs)

    def get_files(
        self,
        dates: Iterable[Union[str, datetime]],
        datatype: Union[Enum, str],
        **kwargs,
    ) -> List[Path]:
        """
        Download files from a list of dates and receives a list pointing to the files.
        If there is a problem during the download of one file, None will be appended to the list
        and an error message error will be in the log.
        """
        files = []
        for date in dates:
            try:
                file = self.get_file(
                    date=date,
                    datatype=datatype,
                    **kwargs,
                )
            except Exception as e:  # pylint: disable=broad-except
                self._logger.error(e)
                files.append(None)
            else:
                files.append(file)

        return files

    def get_range(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        datatype: Union[Enum, str],
        **kwargs,
    ) -> List[Path]:
        """
        Download a range of files from start to end dates and receives a list pointing to the files.
        If there is a problem during the download of one file, a message error will be in the list.
        """
        parser = self.get_parser(datatype=datatype)
        dates = parser.dates_range(start_date, end_date)

        return self.get_files(
            dates=dates,
            datatype=datatype,
            **kwargs,
        )

    # -------------------- Data Manipulation Functions --------------------
    def open_file(
        self, date: Union[str, datetime], datatype: Union[Enum, str], **kwargs
    ) -> xr.DataArray:
        """
        Open the file, downloading it, if that's necessary
        """
        file = self.get_file(date=date, datatype=datatype, **kwargs)

        if file is not None:
            ds = xr.open_dataset(file)

            parser = self.get_parser(datatype=datatype)
            if parser.post_proc is not None:
                ds = parser.post_proc(ds)

            return ds[parser.constants["var"]]

        return None

    def create_cube_dates(
        self,
        dates: List[Union[str, datetime]],
        datatype: Union[Enum, str],
        dim_key: Optional[str] = "time",
        **kwargs,
    ):
        """
        Create a cube from a list of dates
        """
        data_arrays = []
        for date in dates:
            dset = self.open_file(date=date, datatype=datatype, **kwargs)

            if dset is None:
                self._logger.error("Could not open file %s, datatype, %s", date, datatype)
            else:
                data_arrays.append(dset)

        cube = xr.concat(data_arrays, dim=dim_key, coords="minimal", compat="override")
        return cube

    def create_cube(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        datatype: Union[Enum, str],
        dim_key: Optional[str] = "time",
        **kwargs,
    ) -> xr.Dataset:
        """
        Create a cube from a range of files
        """
        parser = self.get_parser(datatype=datatype)
        dates = parser.dates_range(start_date, end_date)

        return self.create_cube_dates(
            dates=dates, datatype=datatype, dim_key=dim_key, **kwargs
        )
