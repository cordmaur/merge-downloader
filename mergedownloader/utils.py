"""
Module with several utils used in raindownloader INPEraindownloader package
"""

import io
import subprocess
from enum import Enum

from pathlib import Path
from typing import Union, List, Optional, Tuple, Callable

# from abc import ABC, abstractmethod
import datetime
import calendar

from dateutil import parser
from dateutil.relativedelta import relativedelta

from PIL import Image
import matplotlib.pyplot as plt

import pandas as pd

from shapely import box
import rasterio as rio
import xarray as xr
import rioxarray as xrio
import geopandas as gpd


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


class DateProcessor:
    """Docstring"""

    @staticmethod
    def parse_date(date: Union[str, datetime.datetime]) -> datetime.datetime:
        """Return a date in datetime format, regardless the input [str | datetime]"""
        return date if isinstance(date, datetime.datetime) else parser.parse(date)

    @staticmethod
    def normalize_date(date: Union[str, datetime.datetime]) -> str:
        """
        Parse the date string in any format accepted by dateutil and delivers a date
        in the following format: "YYYYMMDD"
        """
        date = DateProcessor.parse_date(date)

        return date.strftime("%Y%m%d")

    @staticmethod
    def pretty_date(
        date: Union[str, datetime.datetime], format_str: str = "%d-%m-%Y"
    ) -> str:
        """Return the date in a pretty printable format dd/mm/yyyy"""
        date = DateProcessor.parse_date(date)

        return date.strftime(format_str)

    @staticmethod
    def dates_range(
        start_date: Union[str, datetime.datetime],
        end_date: Union[str, datetime.datetime],
        date_freq: DateFrequency,
    ) -> List[str]:
        """Spawn a dates list in normalized format in the desired range"""

        current_date = DateProcessor.parse_date(start_date)
        final_date = DateProcessor.parse_date(end_date)

        # create the step to be applied to the current date
        step = relativedelta(**date_freq.value)  # type: ignore

        # looop through the dates
        dates = []
        while current_date <= final_date:
            if date_freq == DateFrequency.HOURLY:
                dates.append(DateProcessor.pretty_date(current_date, "%Y%m%dT%H%M%S"))
            else:
                dates.append(DateProcessor.normalize_date(current_date))

            current_date += step

        return dates

    @staticmethod
    def month_abrev(date: Union[str, datetime.datetime]) -> str:
        """Return the month as a three-character string"""
        date = DateProcessor.parse_date(date)

        return date.strftime("%b").lower()

    @staticmethod
    def start_end_dates(date: Union[str, datetime.datetime]) -> Tuple[str, str]:
        """Return the first date and last date in a specific month"""
        date = DateProcessor.parse_date(date)
        today = DateProcessor.today()

        # Check if we are in the current month and get the number of days
        if date.year == today.year and date.month == today.month:
            days = today.day
        else:
            _, days = calendar.monthrange(date.year, date.month)

        # Get the first and last day of the month
        first_day = datetime.datetime(date.year, date.month, 1)
        last_day = datetime.datetime(date.year, date.month, days)

        first_day_str = DateProcessor.normalize_date(first_day)
        last_day_str = DateProcessor.normalize_date(last_day)

        return first_day_str, last_day_str

    @staticmethod
    def last_n_months(
        date: Union[str, datetime.datetime],
        lookback: int = 6,
        include_current: bool = True,
    ) -> Tuple[str, str]:
        """
        Return start and end month considering the month of the given date and
        looking back n months
        """
        date = DateProcessor.parse_date(date)
        i = 0 if include_current else 1

        start_date = date - relativedelta(months=lookback - 1 + i)
        end_date = date - relativedelta(months=i)

        start_date_str = f"{start_date.year}-{start_date.month}"
        end_date_str = f"{end_date.year}-{end_date.month}"

        return (start_date_str, end_date_str)

    @staticmethod
    def create_monthly_periods(
        start_date: Union[str, datetime.datetime],
        end_date: Union[str, datetime.datetime],
        month_step: int,
    ) -> List[tuple]:
        """Create monthly periods given a step (e.g, quaterly=3, semestraly=6, yearly=12)"""
        current_date = DateProcessor.parse_date(start_date)
        final_date = DateProcessor.parse_date(end_date)

        periods = []
        while current_date <= final_date:
            start_period = current_date
            end_period = start_period + relativedelta(months=month_step - 1)

            # if the end date for the period is inside the final date, add this period
            if end_period <= final_date:
                periods.append((start_period, end_period))

            # otherwise, quit the loop
            else:
                break

            current_date += relativedelta(months=month_step)

        return periods

    @staticmethod
    def today():
        """Return the current date without the time part"""
        now = datetime.datetime.now()
        return now + relativedelta(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def count_dates(
        start_date: Union[str, datetime.datetime],
        end_date: Union[str, datetime.datetime],
        date_freq: DateFrequency,
    ) -> int:
        """Count the number of days between two dates"""
        dates = DateProcessor.dates_range(start_date, end_date, date_freq)

        return len(dates)


class GISUtil:
    """Helper class for basic GIS operations"""

    @staticmethod
    def create_cube(
        files: List,
        dim_key: Optional[str] = "time",
    ) -> xr.Dataset:
        """
        Stack the images in the list as one XARRAY Dataset cube.
        """

        # first, check if name parser and dimension key are setted correctly
        # if (name_parser is None) ^ (dim_key is None):
        #     raise ValueError("If name parser or dim key is set, both must be setted.")

        # set the stacked dimension name
        dim = "time" if dim_key is None else dim_key

        # create a cube with the files
        data_arrays = [
            xr.open_dataset(file).astype("float32")
            for file in files
            if Path(file).exists()
        ]

        cube = xr.concat(data_arrays, dim=dim)

        # close the arrays
        for array in data_arrays:
            array.close()

        return cube

    @staticmethod
    def bounds(
        shp: gpd.GeoDataFrame,
        percent_buffer: Optional[float] = None,
        fixed_buffer: Optional[float] = None,
    ) -> tuple:
        """
        Return the total bounds of a shape file with a given buffer
        The buffer can be a fixed distance (in projection units)
        or a percentage of the maximum size
        """

        # get the bounding box of the total shape
        bbox = box(*shp.total_bounds)

        if fixed_buffer is not None:
            bbox = bbox.buffer(fixed_buffer)
        elif percent_buffer is not None:
            xmin, ymin, xmax, ymax = bbox.bounds
            delta_x = xmax - xmin
            delta_y = ymax - ymin
            diag = (delta_x**2 + delta_y**2) ** 0.5
            bbox = bbox.buffer(percent_buffer * diag)

        return bbox.bounds

    @staticmethod
    def cut_cube_by_geoms(
        cube: xr.DataArray, geometries: gpd.GeoSeries
    ) -> xr.DataArray:
        """
        Calculate the cube inside the given geometries in the GeoDataFrame.
        The geometries are stored in a GeoSeries from Pandas
        """

        # first make sure we have the same CRS
        if cube.rio.crs != geometries.crs:
            print(f"Coverting shp CRS to {cube.rio.crs}")
            geometries = geometries.to_crs(cube.rio.crs)  # type: ignore
            
        # Let's use clip to ignore data outide the geometry
        clipped = cube.rio.clip(geometries)

        return clipped

    @staticmethod
    def cut_cube_by_bounds(
        cube: xr.DataArray, xmin: float, ymin: float, xmax: float, ymax: float
    ):
        """Cut the cube by the given bounds"""
        return cube.sel(longitude=slice(xmin, xmax), latitude=slice(ymin, ymax))

    @staticmethod
    def get_time_series(
        cube: xr.DataArray,
        shp: gpd.GeoDataFrame,
        reducer: Callable,
        keep_dim: str = "time",  # specify the dimension along with we will retrieve the TS
    ):
        """Get a time series of values within the shape, given a reducer method"""
        if cube.rio.crs != shp.crs:
            print(f"Coverting shp CRS to {cube.rio.crs}")
            shp = shp.to_crs(cube.rio.crs)  # type: ignore

        # clip to the desired area
        area = GISUtil.cut_cube_by_geoms(cube, shp.geometry)

        # get the dimensions to be reduced
        reduce_dims = list(cube.dims)
        reduce_dims.remove(keep_dim)

        series = reducer(area, dim=reduce_dims).to_series()

        return series

    @staticmethod
    def profile_from_xarray(array: xr.DataArray, driver: Optional[str] = "GTiff"):
        """Create a rasterio profile given an rioxarray"""
        profile = dict(
            driver=driver,
            width=array.rio.width,
            height=array.rio.height,
            count=array.rio.count,
            dtype=array.dtype,
            crs=array.rio.crs,
            transform=array.rio.transform(),
            nodata=array.rio.nodata,
        )

        return profile

    @staticmethod
    def grib2tif(grib_file: Union[str, Path], epsg: int = 4326) -> Path:
        """
        Converts a GRIB2 file to GeoTiff and set correct CRS and Longitude
        """
        grib = xrio.open_rasterio(grib_file)  # type: ignore[no-unsized-index]

        grib = grib.rio.write_crs(rio.CRS.from_epsg(epsg))  # type: ignore[attr]

        # save the precipitation raster
        filename = Path(grib_file).with_suffix(FileType.GEOTIFF.value)

        grib[0].rio.to_raster(filename, compress="deflate")

        return filename

    @staticmethod
    def grib2tif_old(grib_file: Union[str, Path], epsg: int = 4326) -> Path:
        """
        Converts a GRIB2 file to GeoTiff and set correct CRS and Longitude
        """
        grib = xrio.open_rasterio(grib_file)  # type: ignore[no-unsized-index]

        # else:
        grib = grib.rio.write_crs(rio.CRS.from_epsg(epsg))  # type: ignore[attr]

        # save the precipitation raster
        filename = Path(grib_file).with_suffix(FileType.GEOTIFF.value)

        grib["prec"].rio.to_raster(filename, compress="deflate")

        return filename

    @staticmethod
    def animate_cube(
        cube: xr.DataArray,
        filename: str,
        shp: Optional[gpd.GeoDataFrame] = None,
        max_quantile: float = 0.999,
        frametime=25,
        buffer_perc=0.1,
        **kwargs,
    ):
        """Create an animated gif from the cube"""

        # check if the filename folder exists
        file = Path(filename)
        if not file.parent.exists():
            raise FileNotFoundError(f"Folder {file.parent.absolute()} does not exist")

        # create a list of images to store each frame
        images = []

        # if a shapefile is given, cut the cube accordingly
        if shp is not None:
            bounds = GISUtil.bounds(shp=shp, percent_buffer=buffer_perc)
            cube = GISUtil.cut_cube_by_bounds(cube, *bounds)

        # after cutting the cube (if demanded) we can calculate vmax
        vmax = cube.quantile(max_quantile)

        # loop through the "time" dimension from the cube
        for time in cube.time.to_numpy():
            # begin by creating a figure
            print(f"Appending: {time}")
            buf = io.BytesIO()
            fig, axes = plt.subplots(num=1)

            cube.sel(time=time).plot(ax=axes, vmax=vmax, **kwargs)  # type: ignore

            if shp is not None:
                shp.plot(ax=axes, edgecolor="firebrick", facecolor="none")

            # use a file-like object as buffer (to avoid saving images to disk)
            fig.savefig(buf, format="png")
            buf.seek(0)

            images.append(Image.open(buf))

            fig.clear()
            axes.clear()

        images[0].save(
            file.with_suffix(".gif"),
            save_all=True,
            append_images=images[1:],
            duration=frametime * len(images),
            loop=0,
        )


class OSUtil:
    """Helper class for OS related functions"""

    @staticmethod
    def get_local_file_info(file_path: Union[str, Path]) -> dict:
        """Get the size and modification time of a local file"""

        # get the status of the file
        stat = Path(file_path).stat()

        local_dt = datetime.datetime.fromtimestamp(stat.st_mtime)

        return {"datetime": local_dt, "size": stat.st_size}

    @staticmethod
    def clear_folder(folder_path: Union[str, Path]):
        """Clear the given folder"""
        folder_path = Path(folder_path).as_posix()
        command = f"rm -rf {folder_path}/*"
        subprocess.run(command, shell=True, check=True)
