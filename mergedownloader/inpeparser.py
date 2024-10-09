"""
List of parsers for the MERGE/INPE structure
"""

from pathlib import Path
from typing import List, Dict
from enum import Enum
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.colors as colors

import xarray as xr

from .parser import DownloaderParser, ProcessorParser
from .utils import DateProcessor, DateFrequency


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


# -------------------- Basic Functions to Correct INPE files --------------------
def grib2_post_proc(dset: xr.Dataset, **_) -> xr.Dataset:
    """Adjust the longitude in INPE's grib2 files and sets the CRS"""

    dset = dset.assign_coords({"longitude": dset.longitude - 360})
    dset = dset.rio.write_crs("epsg:4326")
    return dset


def nc_post_proc(dset: xr.Dataset, **_) -> xr.Dataset:
    """Adjust variable names in the netCDF files and set CRS"""
    if "lon" in dset.dims:
        dset = dset.rename_dims({"lon": "longitude", "lat": "latitude"})
        dset = dset.rename_vars({"lon": "longitude", "lat": "latitude"})

    dset = dset.rio.write_crs("epsg:4326")

    return dset


# -------------------- Parsers for the MERGE/INPE structure --------------------
class DailyParser(DownloaderParser):
    """Daily is the total rainfall for a specific day"""

    constants = {
        "root": "/modelos/tempo/MERGE/GPM",
        "var": "prec",
        "name": "Daily Rain",
        "freq": DateFrequency.DAILY,
        "post_proc": grib2_post_proc,
    }

    def filename(self, date: datetime, **__):
        """Create the filename of the MERGE file, given a specific date"""
        date_str = DateProcessor.normalize_date(date)
        return f"MERGE_CPTEC_{date_str}.grib2"

    def foldername(self, date: datetime, **__):
        """Returns a foldername to store this file. E.g. "DAILY/2000/01"""
        year = str(date.year)
        month = str(date.month).zfill(2)
        return "/".join(["DAILY", year, month])


class DailyAverageParser(DownloaderParser):
    """
    DAILY_AVERAGE was obtained through the average of each day of the year
    considering the years 2000 to 2023 (24 years).

    Example:
    MERGE_CPTEC_12Z01dec.nc =(01dec2000+01dec2001..+..01dec2023) / (2023-2000+1)
    """

    constants = {
        "root": "/modelos/tempo/MERGE/GPM/CLIMATOLOGY",
        "var": "pmed",
        "name": "Daily Average",
        "freq": DateFrequency.DAILY,
        "post_proc": nc_post_proc,
    }

    def filename(self, date: datetime, **__):
        """Daily Average - E.g.: MERGE_CPTEC_12Z09dec.nc refers to nineth of December"""
        day_month = f"{date.day:02d}{DateProcessor.month_abrev(date)}"
        return f"MERGE_CPTEC_12Z{day_month}.nc"

    def foldername(self, *_, **__):
        """Returns a foldername to store this file. E.g. "DAILY/2000/01"""
        return "DAILY_AVERAGE"


class MonthlyAccumYearlyParser(DownloaderParser):
    """
    MONTHLY_ACCUMULATED_YEARLY was obtained by monthly accumulating for each month.
    Example:
    MERGE_CPTEC_acum_dec_2022.nc=(prec_01dec2022+prec_02dec2022+......+prec_31dec2022)
    """

    constants = {
        "root": "/modelos/tempo/MERGE/GPM/CLIMATOLOGY",
        "var": "pacum",
        "name": "Monthly Accumulated Yearly",
        "freq": DateFrequency.MONTHLY,
        "post_proc": nc_post_proc,
    }

    def filename(self, date: datetime, **__):
        """
        Monthly Accumulated Yearly:
        e.g.: MERGE_CPTEC_acum_sep_2001.nc
        """
        month_year = DateProcessor.month_abrev(date) + "_" + str(date.year)
        return f"MERGE_CPTEC_acum_{month_year}.nc"

    def foldername(self, *_, **__):
        return "MONTHLY_ACCUMULATED_YEARLY"


class MonthlyAccumParser(DownloaderParser):
    """MONTHLY_ACCUMULATED was obtained through the average of the MonthlyAccumlatedYearly,
    considering the years 2000 to 2023 (24 years).
    Example:

    MERGE_CPTEC_acum_dec.nc =(acum_dec2000+acum_dec2001+.....+acum_dec2023) / (2023-2000+1)
    """

    constants = {
        "root": "/modelos/tempo/MERGE/GPM/CLIMATOLOGY",
        "var": "precacum",
        "name": "Monthly Accumulated",
        "freq": DateFrequency.MONTHLY,
        "post_proc": nc_post_proc,
    }

    def filename(self, date: datetime, **__):
        """
        Monthly Accumulated - Create the filename fot the Monthly Accumulated files from MERGE/INPE
        E.g.: MERGE_CPTEC_acum_sep.nc
        """
        month_abrev = DateProcessor.month_abrev(date)
        return f"MERGE_CPTEC_acum_{month_abrev}.nc"

    def foldername(self, *_, **__):
        return "MONTHLY_ACCUMULATED"


class YearAccumulatedParser(DownloaderParser):
    """
    YEAR_ACCUMULATED was obtained by accumulating all the days of the year.

    Example:

    MERGE_CPTEC_acum_2022.nc=(prec_01jan2022+...+prec_31dec2022)
    """

    constants = {
        "root": "/modelos/tempo/MERGE/GPM/CLIMATOLOGY",
        "var": "pacum",
        "name": "Year Accumulated",
        "freq": DateFrequency.YEARLY,
        "post_proc": nc_post_proc,
    }

    def filename(self, date: datetime, **__):
        """
        Yearly Accumulated - Create the filename fot the Yearly Accumulated files from MERGE/INPE
            E.g.: MERGE_CPTEC_acum_2003.nc
        """
        return f"MERGE_CPTEC_acum_{date.year}.nc"

    def foldername(self, *_, **__):
        return "YEAR_ACCUMULATED"


# -------------------- Processors for the MONTHLY_AVG_N and MONTHLY_STD_N --------------------
class MonthlyAvgNParser(ProcessorParser):
    """Docstring"""

    constants = {
        "root": None,
        "var": "avg_n",
        "name": "Monthly Average N",
        "freq": DateFrequency.MONTHLY,
        "post_proc": None,
    }

    def filename(self, date: datetime, **kwargs):
        """
        Create the filename fot the Monthly Average N (Moving Average with N steps).
            E.g.: Monthly_AVG_sep_2.nc
        """
        if "n" not in kwargs:
            raise ValueError("Missing n argument in kwargs")

        month_abrev = DateProcessor.month_abrev(date)

        n = kwargs["n"]
        return f"Monthly_AVG_{month_abrev}_N{n}.nc"

    def foldername(self, *_, **kwargs):
        """
        Create the folder name fot the Monthly Average N files.
        """
        if "n" not in kwargs:
            raise ValueError("Missing n argument in kwargs")

        n = kwargs["n"]
        return f"MONTHLY_STATS_N/MONTHLY_STATS_{n}"

    def must_update(self, date: datetime, output_folder: Path, **kwargs) -> bool:
        """
        Check if the file must be updated.
        """
        # This file is computed manually, so we just check if it exists
        local_target = self.local_target(date, output=output_folder, **kwargs)

        if not local_target.exists():
            dtype = self.__class__.__name__
            raise ValueError(
                f"This type of file {dtype} must be pre-computed using the "
                f"StatsCalculator.calc_monthly_avg_std_n() method."
            )

        else:
            return False

    def inform_dependencies(self, date: datetime, **__) -> Dict[Enum, List]:
        """This type is created through the StatsCalculator"""

    def create_file(
        self, date: datetime, dependencies: Dict[Enum, List[xr.DataArray]], **__
    ) -> xr.Dataset:
        """This type is created through the StatsCalculator"""


class MonthlyStdNParser(ProcessorParser):
    """Docstring"""

    constants = {
        "root": None,
        "var": "std_n",
        "name": "Monthly Std N",
        "freq": DateFrequency.MONTHLY,
        "post_proc": None,
    }

    def filename(self, date: datetime, **kwargs):
        """
        Create the filename fot the Monthly Std N (Moving Average with N steps).
            E.g.: Monthly_STD_sep_2.nc
        """
        if "n" not in kwargs:
            raise ValueError("Missing n argument in kwargs")

        month_abrev = DateProcessor.month_abrev(date)

        n = kwargs["n"]
        return f"Monthly_STD_{month_abrev}_N{n}.nc"

    def foldername(self, *_, **kwargs):
        """
        Create the folder name fot the Monthly Std N files (Moving Average with N steps).
        """
        if "n" not in kwargs:
            raise ValueError("Missing n argument in kwargs")

        n = kwargs["n"]
        return f"MONTHLY_STATS_N/MONTHLY_STATS_{n}"

    def must_update(self, date: datetime, output_folder: Path, **kwargs) -> bool:
        """
        Check if the file must be updated.
        """
        # This file is computed manually, so we just check if it exists
        local_target = self.local_target(date, output=output_folder, **kwargs)

        if not local_target.exists():
            dtype = self.__class__.__name__
            raise ValueError(
                f"This type of file {dtype} must be pre-computed using the "
                f"StatsCalculator.calc_monthly_avg_std_n() method."
            )

        else:
            return False

    def inform_dependencies(self, date: datetime, **__) -> Dict[Enum, List]:
        """This type is created through the StatsCalculator"""

    def create_file(
        self, date: datetime, dependencies: Dict[Enum, List[xr.DataArray]], **__
    ) -> xr.Dataset:
        """This type is created through the StatsCalculator"""


# -------------------- Processors  --------------------
class MonthlyAccumManual(ProcessorParser):
    """Docstring"""

    constants = {
        "root": None,
        "var": "pacum",
        "name": "Monthly Accumulated Manual",
        "freq": DateFrequency.MONTHLY,
        "post_proc": None,
    }

    def filename(self, date: datetime, **_):
        month_year = DateProcessor.month_abrev(date) + "_" + str(date.year)
        return f"MERGE_CPTEC_acum_{month_year}.nc"

    def foldername(self, *_, **__):
        return "MONTHLY_ACCUM_MANUAL"

    def inform_dependencies(self, date: datetime, **__) -> Dict[Enum, List[str]]:
        """
        The MonthlyAccumManual processor depends on the daily information for every single
        day in the given Month/Year. So, here we have to return the list of all dates we need
        from the caller Downloader.
        Returns: dictionary with a data type and a list of dates
        """

        # get first and end date given the reference date (month/year)
        start_date, end_date = DateProcessor.start_end_dates(date=date)

        # get all days in the month/year
        dates = DailyParser.dates_range(start_date, end_date)

        # Return the list of dependencies
        return {InpeTypes.DAILY_RAIN: dates}

    def create_file(
        self, date: datetime, dependencies: Dict[Enum, List[xr.DataArray]], **__
    ) -> xr.Dataset:

        # create a cube with the daily rain
        cube = xr.concat(dependencies[InpeTypes.DAILY_RAIN], dim="time")

        # accumulate the rain
        accum = cube.sum(dim="time")

        # Adjust name and time coordinates
        accum = accum.rename(self.constants["var"])
        ref_time = cube.time[0].values
        accum = accum.assign_coords({"time": ref_time}).expand_dims(dim="time")

        # Convert to dataset and adjust additional attributes
        dset = accum.to_dataset()
        last_day = cube.time[-1].values.astype("datetime64[s]").item()
        dset.attrs["updated"] = str(datetime.now())
        dset.attrs["last_day"] = DateProcessor.normalize_date(last_day)
        dset.attrs["days"] = len(cube.time)

        return dset


class SPI1Processor(ProcessorParser):  # pylint: disable=C0103
    """Docstring"""

    constants = {
        "root": None,
        "var": "SPI1",
        "name": "Standardized Precipitation Index (1m)",
        "freq": DateFrequency.MONTHLY,
        "post_proc": None,
    }

    def filename(self, date: datetime, **_):
        month_year = DateProcessor.pretty_date(date, "%Y-%m")
        return f"SPI1_{month_year}.nc"

    def foldername(self, *_, **__):
        return "MONTHLY_SPI1"

    def inform_dependencies(self, date: datetime, **__) -> Dict[Enum, List]:
        """Docstring"""

        today = DateProcessor.today()
        if today.year == date.year and today.month == date.month:
            raise ValueError("It's not possible to calulate SPI for the current month")

        # Infer the dates needed to calculate the SPI for the given date.
        start_year = 2001
        end_year = DateProcessor.today().year
        month = date.month

        dates = []
        for year in range(start_year, end_year):
            _date = f"{year}-{month:02}-01"
            dates.append(_date)

        # Return the list of dependencies
        # Besides the Monthly Accumulated, we need the rain for the given month
        return {
            InpeTypes.MONTHLY_ACCUM_YEARLY: dates,
            InpeTypes.MONTHLY_ACCUM_MANUAL: [date],
        }

    def create_file(
        self, date: datetime, dependencies: Dict[Enum, List], **__
    ) -> xr.Dataset:
        """Docstring"""

        # create a cube with the monthly rain
        cube = xr.concat(dependencies[InpeTypes.MONTHLY_ACCUM_YEARLY], dim="time")

        # grab the accumulated rain for the given month
        rain = dependencies[InpeTypes.MONTHLY_ACCUM_MANUAL][0]

        # calculate mean and standard deviation along the time axis
        mean = cube.mean(dim="time")
        std = cube.std(dim="time")

        # adjust the projections and coordinates
        rain = rain.rio.reproject_match(std)
        rain = rain.rename({"x": "longitude", "y": "latitude"})

        # calculate SPI
        spi = (rain - mean) / std

        # Adjust name and time coordinates
        spi = spi.rename(self.constants["var"])
        # ref_time = date + relativedelta(day=1)
        # spi = spi.assign_coords({"time": date}).expand_dims(dim="time")

        # Convert to dataset and adjust additional attributes
        dset = spi.to_dataset()
        dset.attrs["updated"] = str(datetime.now())
        dset.attrs["last_day"] = "NA"
        dset.attrs["days"] = "NA"

        return dset


class SPIProcessor(ProcessorParser):
    """Docstring"""

    constants = {
        "var": "SPI",
        "name": "Standardized Precipitation Index",
        "freq": DateFrequency.MONTHLY,
        "post_proc": None,
    }

    #  pylint: disable=arguments-differ
    def filename(self, date: datetime, n: int):
        """
        Return the name for the SPI file, given a date (Year and Month) and the
        number of months to be accumulated (N value)
        """
        month_year = DateProcessor.pretty_date(date, "%Y-%m")
        return f"SPI_{n}_{month_year}.nc"

    def foldername(self, *_, n: int, **__):
        """
        Return the foldername for the SPI file, given the number of months to be
        accumulated (N value)
        """
        return f"MONTHLY_SPI{n}"

    def inform_dependencies(self, date: datetime, n: int) -> Dict[Enum, List[dict]]:
        """Docstring"""

        # First, raise an error if we are trying to calculate SPI for a future date
        today = DateProcessor.today()
        if today.year == date.year and today.month == date.month:
            raise ValueError("It's not possible to calulate SPI for the current month")
        elif date >= today:
            raise ValueError("It's not possible to calulate SPI for future dates")

        # Then, infer the files needed to calculate the SPI for the given date.
        dependencies = {}

        # We need average and standard deviation for the desired month, considering `n`
        dependencies[InpeTypes.MONTHLY_AVG_N] = [{"date": date, "n": n}]
        dependencies[InpeTypes.MONTHLY_STD_N] = [{"date": date, "n": n}]

        # Now, get the months needed for the MONTHLY_ACCUM_MANUAL
        start_month, end_month = DateProcessor.last_n_months(date, lookback=n)
        dates = DateProcessor.dates_range(start_month, end_month, DateFrequency.MONTHLY)
        dependencies[InpeTypes.MONTHLY_ACCUM_MANUAL] = dates

        return dependencies

    #  pylint: enable=arguments-differ

    def create_file(
        self, date: datetime, dependencies: Dict[Enum, List[xr.DataArray]], **__
    ) -> xr.Dataset:

        # Get the average and standar deviation from the dependencies
        avg = dependencies[InpeTypes.MONTHLY_AVG_N][0].squeeze()
        std = dependencies[InpeTypes.MONTHLY_STD_N][0].squeeze()

        # create a cube with the monthly rain
        rain = xr.concat(dependencies[InpeTypes.MONTHLY_ACCUM_MANUAL], dim="time")
        rain = rain.mean(dim="time")
        rain = rain.rio.reproject_match(avg)
        rain = rain.rename({"x": "longitude", "y": "latitude"})

        # calculate SPI
        spi = (rain - avg) / std
        spi = spi.rename(self.constants["var"])
        ref_time = date + relativedelta(day=1)
        spi = spi.assign_coords({"time": ref_time}).expand_dims(dim="time")

        # Convert to dataset and adjust additional attributes
        dset = spi.to_dataset()
        dset.attrs["updated"] = str(datetime.now())
        dset.attrs["last_day"] = "NA"
        dset.attrs["days"] = "NA"

        return dset


# -------------------- Bind the data types to corresponding parsers --------------------
class InpeTypes(Enum):
    """Data types available in the parsers"""

    DAILY_RAIN = "DAILY_RAIN"
    MONTHLY_ACCUM_YEARLY = "MONTHLY_ACCUM_YEARLY"
    DAILY_AVERAGE = "DAILY_AVERAGE"
    MONTHLY_ACCUM = "MONTHLY_ACCUM"
    MONTHLY_ACCUM_MANUAL = "MONTHLY_ACCUM_MANUAL"
    YEARLY_ACCUM = "YEARLY_ACCUM"
    HOURLY_WRF = "HOURLY_WRF"
    MONTHLY_AVG_N = "MONTHLY_AVG_N"
    MONTHLY_STD_N = "MONTHLY_STD_N"
    MONTHLY_SP1 = "MONTHLY_SP1"
    MONTHLY_SPI = "MONTHLY_SPI"


InpeParsers = {
    InpeTypes.DAILY_RAIN: DailyParser(),
    InpeTypes.MONTHLY_ACCUM_YEARLY: MonthlyAccumYearlyParser(),
    InpeTypes.DAILY_AVERAGE: DailyAverageParser(),
    InpeTypes.MONTHLY_ACCUM: MonthlyAccumParser(),
    InpeTypes.MONTHLY_ACCUM_MANUAL: MonthlyAccumManual(),
    InpeTypes.YEARLY_ACCUM: YearAccumulatedParser(),
    InpeTypes.MONTHLY_SP1: SPI1Processor(),
    InpeTypes.MONTHLY_AVG_N: MonthlyAvgNParser(),
    InpeTypes.MONTHLY_STD_N: MonthlyStdNParser(),
    InpeTypes.MONTHLY_SPI: SPIProcessor(),
    # InpeTypes.HOURLY_WRF: None,
}

INPE_SERVER = "ftp.cptec.inpe.br"

__all__ = [
    "DailyParser",
    "DailyAverageParser",
    "MonthlyAccumParser",
    "MonthlyAccumYearlyParser",
    "YearAccumulatedParser",
    "MonthlyAccumManual",
    "InpeTypes",
    "InpeParsers",
    "INPE_SERVER",
]
