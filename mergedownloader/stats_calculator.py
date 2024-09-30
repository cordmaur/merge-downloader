"""
StatsCalculator class
"""

from datetime import datetime

import xarray as xr

from .downloader import Downloader
from .inpeparser import InpeTypes
from .utils import DateProcessor


class StatsCalculator:
    """
    StatsCalculator class

    This class calculates statistics that are used for some variables such as the SPI.
    To provide SPI-N, we need to calculate a moving average of the N - monthly accumulated.
    This is compational intensive and can be done using the StatsCalculator class.

    Keep in mind that the moving average needs to be calculated for every day in the month.
    Additionally, the InpeType.MONTHLY_AVG_N and the InpeType.MONTHLY_STD_N, exists to retrieve
    these values once they are calculated.

    Example:
        >>> from mergedownloader.stats_calculator import StatsCalculator
        >>> stats_calculator = StatsCalculator()
        >>> stats_calculator.calculate_stats("2022-01-01", "2022-01-31", "SPI")
        >>> print(stats_calculator.stats)
    """

    def __init__(self, downloader: Downloader):
        self.downloader = downloader

        # for simplicity, let's use the downloader logger
        self._logger = self.downloader._logger

    def _save_stats(
        self, arr: xr.DataArray, date: datetime, datatype: InpeTypes, **kwargs
    ):
        """
        Save the stats file to disk. But add the mandatory attributes and
        dimensions before saving.
        The local target is taken from the datatype.
        """

        # get the local_target from the downloader
        local_target = self.downloader.local_target(
            date=date, datatype=datatype, **kwargs
        )

        parser = self.downloader.get_parser(datatype)


        # Add the time dimension
        arr = arr.assign_coords({"time": date}).expand_dims(dim="time")

        # Correct the name
        arr = arr.rename(parser.constants["var"])

        # Convert to dataset and adjust additional attributes
        dset = arr.to_dataset()
        dset.attrs["updated"] = str(datetime.now())
        dset.attrs["last_day"] = "NA"
        dset.attrs["days"] = "NA"

        # save the file
        self._logger.info(f"Saving STATS: {local_target}")
        arr.to_netcdf(local_target)


    def calc_monthly_avg_std_n(self, n: int):
        """
        Calculates the monthly average of the N - monthly accumulated
        """

        self._logger.info(f"Calculating Monthly Accumulated and STD ({n})")

        # we cannot calculate with data from current year
        last_year = DateProcessor.today().year - 1

        # let's first create a cube with the MONTHLY_ACCUM_YEARLY
        cube = self.downloader.create_cube(
            start_date="2000-01",
            end_date=f"{last_year}-12",
            datatype=InpeTypes.MONTHLY_ACCUM_YEARLY,
        )

        # calculate the moving average considering the last N months
        moving_average = cube.rolling(time=n).mean()

        # remove the dates where we don't have the moving window calculated
        moving_average = moving_average.dropna("time", how="all")

        # now we can group by month, to calculate the average and standar deviation
        grouped_avg = moving_average.groupby("time.month").mean("time")
        grouped_std = moving_average.groupby("time.month").std("time")

        # we shall have 12 values in each cube, 1 for each month
        # here we have to loop trhough them to save in the filesystem structure

        for month in range(1, 13):
            # setup the date... it can have any year, so we will use the last year it was calculated
            date = DateProcessor.parse_date(f"{last_year}-{month:02}-01")

            self._save_stats(
                arr=grouped_avg.sel(month=month), 
                date=date, 
                datatype=InpeTypes.MONTHLY_AVG_N,
                n=n
            )
            self._save_stats(
                arr=grouped_std.sel(month=month), 
                date=date, 
                datatype=InpeTypes.MONTHLY_STD_N,
                n=n
            )
