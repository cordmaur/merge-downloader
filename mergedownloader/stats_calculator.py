"""
StatsCalculator class
"""

from .downloader import Downloader
from .file_downloader import FileDownloader
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

    def __init__(self, downloader: Downloader, file_downloader: FileDownloader):
        self.downloader = downloader
        self.file_downloader = file_downloader

        # for simplicity, let's use the downloader logger
        self._logger = self.downloader._logger

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
        moving_average = moving_average.dropna('time', how='all')

        # now we can group by month, to calculate the average and standar deviation
        grouped_avg = moving_average.groupby('time.month').mean('time')
        grouped_std = moving_average.groupby('time.month').std('time')

        # we shall have 12 values in each cube, 1 for each month 
        # here we have to loop trhough them to save in the filesystem structure
        for month in range(1, 13):
            pass

