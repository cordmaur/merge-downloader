from enum import Enum


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


class DownloadMode(Enum):
    """Enum to specify download mode:
    - FORCE: if the file already exists, it will be overwritten
    - UPDATE: if the file already exists, update it if necessary (default)
    - NO_UPDATE: if the file already exists, it will not be downloaded
    """

    FORCE = "FORCE"
    UPDATE = "UPDATE"
    NO_UPDATE = "NO_UPDATE"


class InpeTypes(Enum):
    """Data types available in the parsers"""

    DAILY_RAIN = "DAILY_RAIN"
    MONTHLY_ACCUM_YEARLY = "MONTHLY_ACCUM_YEARLY"
    DAILY_AVERAGE = "DAILY_AVERAGE"
    MONTHLY_ACCUM = "MONTHLY_ACCUM"
    MONTHLY_ACCUM_MANUAL = "MONTHLY_ACCUM_MANUAL"
    YEARLY_ACCUM = "YEARLY_ACCUM"
    HOURLY_WRF = "HOURLY_WRF"
    DAILY_WRF = "DAILY_WRF"
    MONTHLY_AVG_N = "MONTHLY_AVG_N"
    MONTHLY_STD_N = "MONTHLY_STD_N"
    MONTHLY_SP1 = "MONTHLY_SP1"
    MONTHLY_SPI = "MONTHLY_SPI"
