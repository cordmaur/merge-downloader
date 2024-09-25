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
from enum import Enum
from pathlib import Path
from typing import Union, Dict, List
from datetime import datetime, timedelta

import xarray as xr

from .utils import DateProcessor, DateFrequency


class AbstractParser(ABC):
    """
    Abstract class for parsers and processors.
    Processors are a specific type of parser, that does not have e remote folder / target.
    Instead, it calculates it and saves it locally, "on demand".
    """

    def __init__(self):
        # setup a logger
        self.logger = logging.getLogger(self.__class__.__name__)

        # check if the instance has all the necessary keys in the constants
        assert set(set(["root", "var", "name", "freq"])).issubset(self.constants.keys())

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
        if "post_proc" in self.constants and self.constants["post_proc"] is not None:
            return self.constants["post_proc"](ds)
        else:
            return ds

    def remote_folder(self, date: datetime, **kwargs) -> str:
        """Return just the remote folder given a date string"""
        if self.constants["root"] is not None:
            return Path(self.constants["root"]) / self.foldername(date, **kwargs)

    def remote_target(self, date: datetime, **kwargs) -> str:
        """Target is composed by root / folder / filename"""
        if self.remote_folder(date, **kwargs) is not None:
            return Path(self.remote_folder(date, **kwargs)) / self.filename(
                date, **kwargs
            )

    def local_folder(
        self, date: datetime, output_folder: Union[Path, str], **kwargs
    ) -> Path:
        """Return the full path of the local file, given a date"""
        folder = Path(output_folder) / self.foldername(date, **kwargs)
        if not folder.exists():
            folder.mkdir(parents=True)
        return folder

    def local_target(self, date: datetime, output: Path, **kwargs) -> Path:
        """Return the local Path + Filename"""
        local_folder = self.local_folder(date=date, output_folder=output, **kwargs)
        local_target = local_folder / self.filename(date=date, **kwargs)

        return local_target

    @classmethod
    def dates_range(cls, start_date: datetime, end_date: datetime) -> list:
        """Return a list of dates between start_date and end_date"""
        return DateProcessor.dates_range(
            start_date=start_date, end_date=end_date, date_freq=cls.constants["freq"]
        )

    def __repr__(self):
        s = f"Instance of {self.__class__.__name__}"
        return s


class AbstractProcessor(AbstractParser):
    """
    Abstract class for parsers and processors.
    Processors are a specific type of parser, that does not have e remote folder / target.
    Instead, it calculates it and saves it locally, "on demand".
    """

    @abstractmethod
    def inform_dependencies(self, date: datetime, **kwargs) -> dict[Enum, List]:
        """
        Returns the dependencies needed by the processor to calculate the variable.
        The dependencies will be fetched by the Downloader (caller) and the result will be passed
        as argument to the create_file method.

        Returns:
            A dictionary with the datatype and the list of dates
            Example: {InpeType.DAILY_RAIN: ['2022-01-01', '2022-01-02', '2022-01-03']}
        """

    def must_update(self, date: datetime, output_folder: Path, **kwargs) -> bool:
        """Decide if the desired file must be reprocessed."""

        # First, get the local target to the file
        local_target = self.local_target(date=date, output=output_folder, **kwargs)
        self.logger.debug("Checking if %s needs update.", local_target)

        # if the file does not exist, exit with True
        if not local_target.exists():
            self.logger.debug("File %s does not exist.", local_target)
            return True

        # Then check if the file has the new attributes
        try:
            dset = xr.open_dataset(local_target)

            # If it's lacking any attribute, return True
            if (
                ("updated" not in dset.attrs)
                or ("days" not in dset.attrs)
                or ("last_day" not in dset.attrs)
            ):
                self.logger.debug(
                    "Forcing update for date %s to add the new attributes ", date
                )
                return True

            # If the file has the attributes, let's check for the dates
            # here we have two options for the file. If it is already complete, with all the days
            # or if it is still missing some day. Let's check that.
            start, end = DateProcessor.start_end_dates(date=date)
            ref_days = DateProcessor.count_dates(start, end, DateFrequency.DAILY)
            updated = DateProcessor.parse_date(dset.attrs["updated"])
            update_delta = datetime.now() - updated

            # Here, if the attribute is NA (Not Applicable), we can assume that the file is complete
            if dset.attrs["days"] != "NA" and dset.attrs["days"] != ref_days:
                self.logger.debug("File not complete on date %s", date)

                # if the file is not complete, give just a 30 min wait to try to update again
                if update_delta < timedelta(minutes=30):
                    self.logger.debug("It's been updated recently. Skipping process.")
                    return False
                else:
                    self.logger.debug("Trying to reprocess it")
                    return True

            # if the file is complete, check how old is the last update
            else:
                if update_delta > timedelta(days=2):
                    self.logger.debug("Last update more than 2 day ago. Forcing update")
                    return True

                return False

        except Exception as error:  # pylint: disable=broad-except
            self.logger.error(error)
            return True

    @abstractmethod
    def create_file(
        self, date: datetime, dependencies: Dict[Enum, List[xr.DataArray]], **kwargs
    ) -> xr.Dataset:
        """todo: Add docstring"""
