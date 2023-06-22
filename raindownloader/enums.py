from enum import Enum, auto

from typing import Union, List


class INPETypes(Enum):
    """Data types available from INPE"""

    DAILY_RAIN = {"id": auto(), "var": "prec", "name": "Daily Rain"}
    MONTHLY_ACCUM_YEARLY = {
        "id": auto(),
        "var": "pacum",
        "name": "Monthly Accumulated Yearly",
    }
    DAILY_AVERAGE = {"id": auto(), "var": "pmed", "name": "Daily Average"}
    MONTHLY_ACCUM = {"id": auto(), "var": "precacum", "name": "Monthly Accumulated"}
    MONTHLY_ACCUM_MANUAL = {
        "id": auto(),
        "var": "monthacum",
        "name": "Monthly Accumulated Yearly",
    }
    YEARLY_ACCUM = {"id": auto(), "var": "pacum", "name": "Year Accumulated"}
    HOURLY_WRF = {
        "id": auto(),
        "var": "hour_wrf",
        "name": "Hourly Forecast (7day model)",
    }
    DAILY_WRF = {"id": auto(), "var": "forecast", "name": "Daily Forecast (7day model)"}

    @classmethod
    def from_name(cls, name_str):
        """Get enum member from its name"""
        try:
            return cls[name_str]

        except KeyError as exc:
            raise ValueError(f"Unknown name: {name_str}") from exc
            # raise ValueError(f"Unknown name: {name_str}")

    @classmethod
    def types(cls, as_string=True) -> Union[List[str], str]:
        """Return available types in str format"""
        lst = [inpe_type.name for inpe_type in cls]

        if as_string:
            return ", ".join(inpe_type.name for inpe_type in cls)
        else:
            return lst


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
