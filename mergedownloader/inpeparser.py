"""
List of parsers for the MERGE/INPE structure
"""

from enum import Enum, auto
from datetime import datetime

import xarray as xr

from .parser import AbstractParser
from .utils import DateProcessor, DateFrequency


# ----- Basic Functions to Correct INPE files -----
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


# ----- Parsers for the MERGE/INPE structure -----
class DailyParser(AbstractParser):
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
        year = str(date.year)
        month = str(date.month).zfill(2)
        return "/".join(["DAILY", year, month])


class DailyAverageParser(AbstractParser):
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
        return "DAILY_AVERAGE"


class MonthlyAccumYearlyParser(AbstractParser):
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


class MonthlyAccumParser(AbstractParser):
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


class YearAccumulatedParser(AbstractParser):
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


# ----- Bind the data types to corresponding parsers -----
class InpeTypes(Enum):
    """Data types available in the parsers"""

    DAILY_RAIN = auto()
    MONTHLY_ACCUM_YEARLY = auto()
    DAILY_AVERAGE = auto()
    MONTHLY_ACCUM = auto()
    MONTHLY_ACCUM_MANUAL = auto()
    YEARLY_ACCUM = auto()
    HOURLY_WRF = auto()


InpeParsers = {
    InpeTypes.DAILY_RAIN: DailyParser(),
    InpeTypes.MONTHLY_ACCUM_YEARLY: MonthlyAccumYearlyParser(),
    InpeTypes.DAILY_AVERAGE: DailyAverageParser(),
    InpeTypes.MONTHLY_ACCUM: MonthlyAccumParser(),
    # InpeTypes.MONTHLY_ACCUM_MANUAL: None,
    InpeTypes.YEARLY_ACCUM: YearAccumulatedParser(),
    # InpeTypes.HOURLY_WRF: None,
}

INPE_SERVER = "ftp.cptec.inpe.br"

__all__ = [
    "DailyParser",
    "DailyAverageParser",
    "MonthlyAccumParser",
    "MonthlyAccumYearlyParser",
    "YearAccumulatedParser",
    "InpeTypes",
    "InpeParsers",
    "INPE_SERVER",
]
