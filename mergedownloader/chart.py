from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

from mergedownloader.inpeparser import InpeTypes, InpeParsers


class ChartUtil:
    """ChartUtil class"""
    @staticmethod
    def save_bar(series: pd.Series, datatype: InpeTypes, filename: str):
        """Save a bar chart to a file"""

        # check if the folder exists
        file = Path(filename)

        if not file.parent.exists():
            raise FileNotFoundError(
                f"Folder '{file.parent.absolute()}' does not exist."
            )

        axes = ChartUtil.bar_chart(series=series, datatype=datatype)

        axes.figure.subplots_adjust(bottom=0.3)
        axes.figure.savefig(file.with_suffix(".png").as_posix())

    @staticmethod
    def bar_chart(series: pd.Series, datatype: InpeTypes) -> plt.Axes:
        """Create a bar chart from a given series"""

        # plot a graph
        _, axes = plt.subplots(num=2)
        axes.bar(x=series.index.strftime("%d-%m-%Y"), height=series.values)  # type: ignore

        labels = axes.get_xticks()
        axes.xaxis.set_major_locator(plt.FixedLocator(labels))  # type: ignore
        axes.set_xticklabels(labels, rotation=90)

        title = InpeParsers[datatype].constants['name']
        axes.set_ylabel("Precipitaion (mm)")
        axes.set_title(title)



        return axes
