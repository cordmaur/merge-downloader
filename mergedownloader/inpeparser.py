"""
Module with specialized classes to understand the INPE FTP Structure
The idea is to have several classes that implement the following interface:
remote_file_path(date: str)
"""
import os

# from abc import ABC, abstractmethod
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Optional, Union, List

import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import matplotlib.colors as colors

import xarray as xr

from .parser import BaseParser
from .utils import DateProcessor, FTPUtil, GISUtil, OSUtil, INPETypes
from .enums import DateFrequency, INPETypes


class INPE:
    """Create the structure, given a root path (remote or local) and date/time of the file"""

    # DailyMERGEroot = "/modelos/tempo/MERGE/GPM/DAILY"

    # Define the colors and positions of the color stops
    cmap_colors = [(1.0, 1.0, 1.0), (1, 1, 1.0), (0.5, 0.5, 1.0), (1.0, 0.4, 0.6)]
    positions = [0.0, 0.1, 0.7, 1.0]

    # Create the colormap using LinearSegmentedColormap
    cmap = colors.LinearSegmentedColormap.from_list(
        "my_colormap", list(zip(positions, cmap_colors))
    )

    def __init__(self, root_path: str) -> None:
        self.root = os.path.normpath(root_path)

    @staticmethod
    def MERGE_structure(date: datetime) -> str:  # pylint: disable=invalid-name
        """Given a date that can be parsed by dateutil, create the structure of the MERGE files"""
        year = str(date.year)
        month = str(date.month).zfill(2)
        return "/".join([year, month])

    @staticmethod
    def MERGE_filename(date: datetime) -> str:  # pylint: disable=invalid-name
        """Create the filename of the MERGE file, given a specific date"""
        date_str = DateProcessor.normalize_date(date)
        return f"MERGE_CPTEC_{date_str}.grib2"

    @staticmethod
    def MERGE_MAY_filename(date: datetime) -> str:  # pylint: disable=invalid-name
        """
        Monthly Accumulated Yearly:
        Create the filename of the MERGE file, given a specific date
        """
        month_year = DateProcessor.month_abrev(date) + "_" + str(date.year)
        return f"MERGE_CPTEC_acum_{month_year}.nc"

    @staticmethod
    def MERGE_DA_filename(date: datetime) -> str:  # pylint: disable=invalid-name
        """
        Daily Average
        Create the filename of the MERGE file, given a specific date
        """
        day_month = f"{date.day:02d}{DateProcessor.month_abrev(date)}"
        return f"MERGE_CPTEC_12Z{day_month}.nc"

    @staticmethod
    def MERGE_MA_filename(date: datetime) -> str:  # pylint: disable=invalid-name
        """
        Monthly Accumulated - Create the filename fot the Monthly Accumulated files from MERGE/INPE
        E.g.: MERGE_CPTEC_acum_sep.nc
        """
        month_abrev = DateProcessor.month_abrev(date)
        return f"MERGE_CPTEC_acum_{month_abrev}.nc"

    @staticmethod
    def MERGE_YA_filename(date: datetime) -> str:  # pylint: disable=invalid-name
        """
        Yearly Accumulated - Create the filename fot the Yearly Accumulated files from MERGE/INPE
            E.g.: MERGE_CPTEC_acum_2003.nc
        """
        return f"MERGE_CPTEC_acum_{date.year}.nc"

    @staticmethod
    def yearly_post_proc(dset: xr.Dataset, date_str, **_) -> xr.Dataset:
        """Adjust the time for the dataset"""

        # first, perform the same adjust for all NCs
        dset = INPE.nc_post_proc(dset)

        date = DateProcessor.parse_date(date_str)
        # fix month and day as 1
        date = date + relativedelta(day=1, month=1)

        return dset.assign_coords({"time": [date]})

    @staticmethod
    def grib2_post_proc(dset: xr.Dataset, **_) -> xr.Dataset:
        """Adjust the longitude in INPE's grib2 files and sets the CRS"""

        dset = dset.assign_coords({"longitude": dset.longitude - 360})
        dset = dset.rio.write_crs("epsg:4326")
        return dset

    @staticmethod
    def nc_post_proc(dset: xr.Dataset, **_) -> xr.Dataset:
        """Adjust variable names in the netCDF files and set CRS"""
        if "lon" in dset.dims:
            dset = dset.rename_dims({"lon": "longitude", "lat": "latitude"})
            dset = dset.rename_vars({"lon": "longitude", "lat": "latitude"})

        dset = dset.rio.write_crs("epsg:4326")

        return dset

    @staticmethod
    def WRF_foldername(date: datetime, **kwargs) -> str:  # pylint: disable=invalid-name
        """
        Given a date that can be parsed by dateutil, create the structure of the WRF files
        E.g., 2023/05/22/00
        """
        date = DateProcessor.parse_date(kwargs["ref_date"])
        year = str(date.year)
        month = str(date.month).zfill(2)
        day = str(date.day).zfill(2)

        return "/".join([year, month, day, "00"])

    @staticmethod
    def WRF_filename(  # pylint: disable=invalid-name
        date: datetime, ref_date: Union[str, datetime]
    ) -> str:
        """
        WRF Hourly Filename - Create the filename fot the Hourly Forecast from WRF/INPE
            E.g.: WRF_cpt_07KM_2023052200_2023052312.grib2
        """
        date1 = DateProcessor.normalize_date(ref_date) + "00"

        date2 = DateProcessor.parse_date(date)
        date2 = DateProcessor.pretty_date(date2, "%Y%m%d%H")
        return f"WRF_cpt_07KM_{date1}_{date2}.grib2"


class MonthAccumParser(BaseParser):
    """Docstring"""

    def __init__(
        self,
        datatype: Union[Enum, str],
        fn_creator: Callable,
        daily_parser: BaseParser,
        fl_creator: Optional[Callable] = None,
        date_freq: DateFrequency = DateFrequency.DAILY,
        ftp: Optional[FTPUtil] = None,
        avoid_update: bool = True,
    ):
        super().__init__(
            datatype=datatype,
            root="",
            filename_fn=fn_creator,
            foldername_fn=fl_creator,
            date_freq=date_freq,
            ftp=ftp,
            avoid_update=avoid_update,
        )

        self.daily_parser = daily_parser

    def download_file(self, date: Union[str, datetime], local_folder: Union[str, Path]):
        """Actually this function performs as accum_monthly_rain"""
        # accumulate the daily rain
        return self.accum_monthly_rain(
            date=date, local_folder=local_folder, force_download=True
        )

    def accum_monthly_rain(
        self,
        date: Union[str, datetime],
        local_folder: Union[str, Path],
        force_download: bool,
    ) -> Path:
        """Docstring"""

        # create a cube with the daily rain in the given month
        start_date, end_date = DateProcessor.start_end_dates(date=date)

        # here, we will check for the current month
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)

        if DateProcessor.parse_date(end_date) >= today:
            # if the end date is future, it meansn we are trying to get the current month
            # in this case, let's start going backwards to see the last available file in the FTP
            for day in range(today.day, 0, -1):
                end_date = today + relativedelta(day=day)
                remote_file = self.daily_parser.remote_target(
                    DateProcessor.normalize_date(end_date)
                )

                # if the file exists, then we have our end date
                if self.ftp.file_exists(remote_file):
                    break

                # otherwise, raise exception if we reached the first day
                if day == 1:
                    raise Exception(f"No avilable file to calculate month {date}")

        end_date = DateProcessor.normalize_date(end_date)

        daily_files = self.daily_parser.get_range(
            start_date=start_date,
            end_date=end_date,
            local_folder=local_folder,
            force_download=force_download,
        )

        dset = GISUtil.create_cube(files=daily_files, dim_key="time")
        cube = INPE.grib2_post_proc(dset)

        # get the reference datetime
        ref_time = cube.time[0].values

        accum = cube[INPETypes.DAILY_RAIN.value["var"]].sum(dim="time")
        accum = accum.rename(INPETypes.MONTHLY_ACCUM_MANUAL.value["var"])

        # once the reduction is being done in the time dimension, create a new dimension for time
        accum = accum.assign_coords({"time": ref_time}).expand_dims(dim="time")

        # save the file to disk
        target_file = self.local_target(date=date, local_folder=local_folder)
        dset = accum.to_dataset()

        # update the creation date for this file
        dset.attrs["updated"] = str(now)
        dset.attrs["last_day"] = end_date
        dset.attrs["days"] = len(daily_files)

        dset.to_netcdf(target_file)

        return target_file

    def get_file(
        self,
        date: Union[str, datetime],
        local_folder: Union[str, Path],
        force_download: bool = False,
    ) -> Path:
        """
        Get a specific file. If it is not available locally, download it just in time.
        If it is available locally and avoid_update is not True, check if the file has
        changed in the server
        """

        must_update = False
        dset = None
        local_target = self.local_target(date=date, local_folder=local_folder)

        self.logger.debug("Getting file %s", local_target.name)

        # first check verifies if the file exists and has the new attributes
        if not local_target.exists() or force_download:
            must_update = True

        else:
            # the file exists, try to open it and check if it is updated
            try:
                dset = xr.open_dataset(local_target)

                if (
                    ("updated" not in dset.attrs)
                    or ("days" not in dset.attrs)
                    or ("last_day" not in dset.attrs)
                ):
                    self.logger.debug(
                        "Forcing update for date %s to add the new attributes ", date
                    )
                    must_update = True

            except Exception as error:
                self.logger.error(error)
                must_update = True

        # now, we have to decide if the file must be updated
        if dset is not None and not must_update:
            # first, let's get the dates from the file
            date = DateProcessor.parse_date(date)
            now = datetime.now()
            last_day = DateProcessor.parse_date(dset.attrs["last_day"])
            updated = DateProcessor.parse_date(dset.attrs["updated"])

            # if file was updated in the last 30 min, return it regardless anything.
            update_delta = now - updated
            if update_delta.seconds < (30 * 60):
                return local_target

            # check if it is complete (has all the necessary days)
            if (date.year == now.year) and (date.month == now.month):
                ref_days = now.day
            else:
                _, ref_days = calendar.monthrange(date.year, date.month)

            if dset.attrs["days"] != ref_days:
                self.logger.debug(
                    "File was created with %s days, expected: %s days",
                    dset.attrs["days"],
                    ref_days,
                )
                must_update = True

            else:
                # now, let's check how far are the days in the past
                timedelta = now - last_day
                if timedelta.days < 30:
                    # file references nearby dates, let's check if it was updated recently
                    self.logger.debug("Month within 30 days limit.")

                    if update_delta.seconds > (2 * 60 * 60):
                        # last update was more than 2 hours ago
                        self.logger.debug(
                            "Last file update was %s. Forcing new update.", updated
                        )
                        must_update = True
                    else:
                        self.logger.debug("File updated recently (%s)", updated)

                else:
                    self.logger.debug(
                        "Monthly file refers to old files. Not necessary to update."
                    )

        # close the dataset
        if dset is not None:
            dset.close()

        if must_update:
            # store the old avoid update status
            avoid_update = self.daily_parser.avoid_update
            self.daily_parser.avoid_update = True  # False

            file = self.accum_monthly_rain(
                date=date,
                local_folder=local_folder,
                force_download=force_download,
            )

            # retrieve the avoid_update status
            self.daily_parser.avoid_update = avoid_update
            return file

        return local_target

        # must_update = False
        # local_target = self.local_target(date=date, local_folder=local_folder)

        # # if the file does not exist or we ask to download it again, we set the must_update flag
        # # to True
        # if not local_target.exists() or force_download:
        #     must_update = True

        # # otherwise we have to open the file and check update conditions
        # else:
        #     dset = xr.open_dataset(local_target)

        #     ### Check attributes to conform the files to the new version
        #     if (
        #         ("updated" not in dset.attrs)
        #         or ("days" not in dset.attrs)
        #         or ("last_day" not in dset.attrs)
        #     ):
        #         self.logger.debug(
        #             "Forcing update for date %s to add the new attributes ", date
        #         )
        #         must_update = True

        #     else:
        #         # get the dates from the file
        #         date = DateProcessor.parse_date(date)
        #         now = datetime.now()
        #         last_day = DateProcessor.parse_date(dset.attrs["last_day"])
        #         updated = DateProcessor.parse_date(dset.attrs["updated"])

        #         # Now, let's treat separately if we are in the current month or not
        #         if (date.year == now.year) and (date.month == now.month):
        #             # if we are in the current month, check the last day used in the file
        #             # force update if last update was more than 2 hour ago.
        #             timedelta = now - updated
        #             if (now.day >= last_day.day) or (timedelta.seconds > 2 * 60 * 60):
        #                 self.logger.debug(
        #                     "Last update was %s minutes ago", timedelta.seconds / 60
        #                 )
        #                 if now.day >= last_day.day:
        #                     self.logger.debug("Last_day %s < today", last_day)

        #                 self.logger.debug("Forcing update")
        #                 must_update = True

        #             else:
        #                 must_update = False

        #         else:
        #             # let's check if the file was updated with all the necessary days.
        #             _, days = calendar.monthrange(date.year, date.month)

        #             # update the file if the number of days differ or if the last day is recent??
        #             if dset["days"] != days:
        #                 self.logger.debug(
        #                     "File was created with %s, expected: %s", dset["days"], days
        #                 )
        #                 must_update = True

        # if must_update:
        #     # store the old avoid update status
        #     avoid_update = self.daily_parser.avoid_update
        #     self.daily_parser.avoid_update = True  # False

        #     file = self.accum_monthly_rain(
        #         date=date,
        #         local_folder=local_folder,
        #         force_download=force_download,
        #     )

        #     # retrieve the avoid_update status
        #     self.daily_parser.avoid_update = avoid_update
        #     return file

        # else:
        #     return local_target

        # # we will force the accum_monthly rain if the month is the current month
        # date = DateProcessor.parse_date(date) + relativedelta(day=1)

        # now = datetime.now() + relativedelta(
        #     day=1, hour=0, minute=0, second=0, microsecond=0
        # )

        # # check if we are in the same month... if that's the case, force the parsers to check if the files
        # # were updated in the server
        # if now == date or force_download or not local_target.exists():
        #     # store the old avoid update status
        #     avoid_update = self.daily_parser.avoid_update
        #     self.daily_parser.avoid_update = True  # False

        #     file = self.accum_monthly_rain(
        #         date=date,
        #         local_folder=local_folder,
        #         force_download=force_download,
        #     )

        #     # retrieve the avoid_update status
        #     self.daily_parser.avoid_update = avoid_update

        #     return file

        # else:
        #     return local_target


class HourlyWRFParser(BaseParser):
    """Docstring"""

    def download_file(
        self,
        date: Union[str, datetime],
        local_folder: Union[str, Path],
        ref_date: Union[str, datetime],
    ) -> Path:
        """Instead of downloading a specific hour, we have to actually calculate it"""
        # convert the date to datetime
        date = DateProcessor.parse_date(date)

        # download this specific date/hour
        file1 = super().download_file(
            date, local_folder=local_folder, ref_date=ref_date
        )

        # download the previous hour
        prev_date = date - timedelta(hours=1)
        tmp_folder = Path(local_folder) / "tmp"
        if not tmp_folder.exists():
            tmp_folder.mkdir(parents=True, exist_ok=True)
        file2 = super().download_file(
            prev_date, local_folder=tmp_folder, ref_date=ref_date
        )

        # open both files and subtract file1 - file2
        dset1 = xr.open_dataset(file1)
        dset2 = xr.open_dataset(file2)
        dset = dset1 - dset2

        # save the hourly rain to disk and delete the temporary
        dset.attrs["updated"] = str(datetime.now())

        dset = dset.rename_vars({"unknown": "hour_wrf"})
        dset = dset.assign_coords({"longitude": dset.longitude - 360})
        dset = dset.rio.write_crs("epsg:4326")

        # close the datasets and clear the temp folder
        dset.to_netcdf(file1)
        # file1 = file1.with_suffix(".tif")
        # dset.to_array().squeeze().rio.to_raster(file1)

        dset1.close()
        dset2.close()
        OSUtil.clear_folder(tmp_folder)

        return file1


class DailyWRFParser(BaseParser):
    """Docstring"""

    @staticmethod
    def _foldername_fn(_: datetime, **kwargs) -> str:
        """Create a foldername for the local storage"""
        return DateProcessor.normalize_date(kwargs["ref_date"])

    @staticmethod
    def _filename_fn(date: datetime, ref_date: Union[str, datetime]) -> str:
        """Create the filename for local storage"""
        date1 = DateProcessor.normalize_date(ref_date)
        date2 = DateProcessor.normalize_date(date)
        return f"WRF_{date1}_{date2}.grib2"

    def __init__(
        self,
        datatype: Union[Enum, str],
        root: str,
        hourly_parser: BaseParser,
        ftp: Optional[FTPUtil] = None,
        avoid_update: bool = True,
        post_proc: Optional[Callable] = None,
    ):
        super().__init__(
            datatype=datatype,
            root=root,
            filename_fn=DailyWRFParser._filename_fn,
            foldername_fn=DailyWRFParser._foldername_fn,
            date_freq=DateFrequency.DAILY,
            ftp=ftp,
            avoid_update=avoid_update,
            mirror_folder=True,
            post_proc=post_proc,
        )

        self.hourly_parser = hourly_parser

        if ftp is not None:
            self.hourly_parser.ftp = ftp

    def download_file(
        self,
        date: Union[str, datetime],
        local_folder: Union[str, Path],
        ref_date: Union[str, datetime],
    ):
        """Actually this function performs as accum_monthly_rain"""
        # accumulate the daily forecast
        return self.accum_daily_forecast(
            date=date, local_folder=local_folder, force_download=True, ref_date=ref_date
        )

    def accum_daily_forecast(
        self,
        date: Union[str, datetime],
        ref_date: Union[str, datetime],
        local_folder: Union[str, Path],
        force_download: bool = False,
    ):
        """Docstring"""

        ### create a CUBE with the hourly forecast in the given date
        # get the hourly dates to process
        date = DateProcessor.parse_date(date).replace(hour=12, minute=0, second=0)

        files = self.hourly_parser.get_range(
            start_date=date - timedelta(hours=23),
            end_date=date,
            local_folder=local_folder,
            force_download=force_download,
            ref_date=ref_date,
        )

        # create the cube and get the correct variable
        cube = GISUtil.create_cube(files=files, dim_key="time")
        if self.hourly_parser.post_proc:
            cube = self.hourly_parser.post_proc(cube)

        accum = cube[self.hourly_parser.varname].sum(dim="time")
        accum = accum.rename(self.datatype.value["var"])  # type: ignore

        # once the reduction is being done in the time dimension, create a new dimension for time
        accum = accum.assign_coords({"time": date}).expand_dims(dim="time")

        # save the file to disk
        target_file = self.local_target(
            date=date, local_folder=local_folder, ref_date=ref_date
        )
        dset = accum.to_dataset()

        # update the creation date for this file
        dset.attrs["updated"] = str(datetime.now())

        dset.to_netcdf(target_file)

        # return self.local_path(date=date)
        return target_file


class INPEParsers:
    """Just a structure to store the parsers for the INPE FTP"""

    FTPurl = "ftp.cptec.inpe.br"

    daily_rain_parser = BaseParser(
        datatype=INPETypes.DAILY_RAIN,
        root="/modelos/tempo/MERGE/GPM/DAILY/",
        filename_fn=INPE.MERGE_filename,
        foldername_fn=INPE.MERGE_structure,
        post_proc=INPE.grib2_post_proc,
    )

    monthly_accum_yearly = BaseParser(
        datatype=INPETypes.MONTHLY_ACCUM_YEARLY,
        root="modelos/tempo/MERGE/GPM/CLIMATOLOGY/MONTHLY_ACCUMULATED_YEARLY/",
        filename_fn=INPE.MERGE_MAY_filename,
        date_freq=DateFrequency.MONTHLY,
        post_proc=INPE.nc_post_proc,
    )

    daily_average = BaseParser(
        datatype=INPETypes.DAILY_AVERAGE,
        root="/modelos/tempo/MERGE/GPM/CLIMATOLOGY/DAILY_AVERAGE",
        filename_fn=INPE.MERGE_DA_filename,
        date_freq=DateFrequency.DAILY,
        post_proc=INPE.nc_post_proc,
    )

    monthly_accum = BaseParser(
        datatype=INPETypes.MONTHLY_ACCUM,
        root="/modelos/tempo/MERGE/GPM/CLIMATOLOGY/MONTHLY_ACCUMULATED/",
        filename_fn=INPE.MERGE_MA_filename,
        date_freq=DateFrequency.MONTHLY,
        post_proc=INPE.nc_post_proc,
    )

    month_accum_manual = MonthAccumParser(
        datatype=INPETypes.MONTHLY_ACCUM_MANUAL,
        fn_creator=INPE.MERGE_MAY_filename,
        date_freq=DateFrequency.MONTHLY,
        daily_parser=daily_rain_parser,
    )

    year_accum = BaseParser(
        datatype=INPETypes.YEARLY_ACCUM,
        root="/modelos/tempo/MERGE/GPM/CLIMATOLOGY/YEAR_ACCUMULATED",
        filename_fn=INPE.MERGE_YA_filename,
        date_freq=DateFrequency.YEARLY,
        post_proc=INPE.yearly_post_proc,
    )

    hourly_wrf = HourlyWRFParser(
        datatype=INPETypes.HOURLY_WRF,
        root="/modelos/tempo/WRF/ams_07km/recortes/prec/",
        filename_fn=INPE.WRF_filename,
        foldername_fn=INPE.WRF_foldername,
        mirror_folder=True,
        date_freq=DateFrequency.HOURLY,
        post_proc=lambda x, **_: x.rio.write_crs("epsg:4326"),
    )

    daily_wrf = DailyWRFParser(
        datatype=INPETypes.DAILY_WRF,
        root="/modelos/tempo/WRF/ams_07km/recortes/prec/",
        hourly_parser=hourly_wrf,
        post_proc=lambda x, **_: x.rio.write_crs("epsg:4326"),
    )

    parsers = [
        daily_rain_parser,
        monthly_accum_yearly,
        daily_average,
        monthly_accum,
        month_accum_manual,
        year_accum,
        hourly_wrf,
        daily_wrf,
    ]

    # post_processors = {".grib2": INPE.grib2_post_proc, ".nc": INPE.nc_post_proc}
