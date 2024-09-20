from enum import Enum, auto

from typing import Union, List


class DateFrequency(Enum):
    """Specifies date frequency for the products"""

    DAILY = {"days": 1}
    MONTHLY = {"months": 1}
    YEARLY = {"years": 1}
    HOURLY = {"hours": 1}


class FileType(Enum):
    """Specifies the file types for downloading"""

    GRIB = ".grib2"
    GEOTIFF = ".tif"
    NETCDF = ".nc"
