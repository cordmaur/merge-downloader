"""
The parser module defines the template of a Parser Class.

The `FileDownloader` is just a dumb class for downloading files from FTP or HTTP.
Someone needs to know exactly where the files are. For example, if we want to download
the monthly average of rainfall, we need to know where is the rainfall data on the server. 

Ex. We want the daily rainfall data for 15/04/2023. This file is located at
https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/DAILY/2023/04/MERGE_CPTEC_20230415.grib2

The parser also prepares the local folder where the downloaded files will be saved.

With that in mind, we will define an AbstractClass that takes care of the targets.

"""

from abc import ABC, abstractmethod
import logging

from pathlib import Path
from typing import Union
from datetime import datetime

import xarray as xr

from .utils import DateProcessor


class AbstractParser(ABC):
    """Abstract class for parsers"""

    def __init__(self):
        # setup a logger
        self.logger = logging.getLogger(self.__class__.__name__)

        # check if the instance has all the necessary keys in the constants
        assert set(self.constants.keys()).issubset(set(["root", "var", "name", "freq"]))

    @property
    @abstractmethod
    def constants(self) -> dict:
        """
        Return constants to be used in the parser. The following keys are required:
        root - the root folder, from which we are going to look for the files
        var - the name of the variable we are extracting from the file
        name - the name of the variable to be displayed in the systems
        freq - the frequency of the data (DateFrequency)
        """

    @abstractmethod
    def filename(self, date: datetime, **kwargs) -> str:
        """Return just the filename for the specific data type, given a date"""

    @abstractmethod
    def foldername(self, date: datetime, **kwargs) -> str:
        """Return foldername for the specific data type, given a date"""

    def post_proc(self, ds: xr.Dataset) -> xr.Dataset:
        """Post process the dataset"""
        if "post_proc" in self.constants:
            return self.constants["post_proc"](ds)
        else:
            return ds

    def remote_folder(self, date: datetime, **kwargs) -> str:
        """Return just the remote folder given a date string"""
        # get the datetime
        return Path(self.constants["root"]) / self.foldername(date, **kwargs)

    def remote_target(self, date: datetime, **kwargs) -> str:
        """Target is composed by root / folder / filename"""
        return Path(self.remote_folder(date, **kwargs)) / self.filename(date, **kwargs)

    def local_folder(
        self, date: datetime, output_folder: Union[Path, str], **kwargs
    ) -> Path:
        """Return the full path of the local file, given a date"""
        folder = Path(output_folder) / self.foldername(date, **kwargs)
        if not folder.exists():
            folder.mkdir(parents=True)
        return folder

    def dates_range(self, start_date: datetime, end_date: datetime) -> list:
        """Return a list of dates between start_date and end_date"""
        return DateProcessor.dates_range(
            start_date=start_date, end_date=end_date, date_freq=self.constants["freq"]
        )

    # def local_target(
    #     self, date: datetime, output_folder: Union[Path, str], **kwargs
    # ) -> Path:
    #     """Return the full path of the local file, given a date"""
    #     folder = self.local_folder(date, output_folder)
    #     filename = self.filename(date, **kwargs)

    #     if not folder.exists():
    #         folder.mkdir(parents=True)

    #     return folder / filename

    def __repr__(self):
        s = f"Instance of {self.__class__.__name__}"
        return s
