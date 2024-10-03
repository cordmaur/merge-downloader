"""
This module has the functions for the Command Line Interface.

The CLI has the following functions:
- init: setup the download folder, and url configurations
- reset: reset default configuration to './' and 'ftp.cptec.inpe.br'
- download: download the data files
- series: Calculate the rain and create a time-series for the specified period


"""
import argparse
from configparser import ConfigParser
from pathlib import Path
from urllib.parse import urlparse

import requests

import geopandas as gpd
import xarray as xr

from mergedownloader.file_downloader import FileDownloader, ConnectionType, DownloadMode
from mergedownloader.inpeparser import InpeParsers, InpeTypes, INPE_SERVER
from mergedownloader.downloader import Downloader
from mergedownloader.utils import DateProcessor, GISUtil
from mergedownloader.chart import ChartUtil


# -------------------- Configuration Methods --------------------
def config_file() -> Path:
    """
    Returns the configuration file path.
    For simplicity, the file will be always located in the package root.
    """
    return Path(__file__).with_name("config.ini")


def open_config() -> ConfigParser:
    """Open the configuration file as a ConfigParser instance"""
    file = config_file()
    if not file.exists():
        raise FileNotFoundError(
            (f"Config file '{file}' not found. Run merge-downloader init first!")
        )

    config = ConfigParser()
    config.read(config_file())

    return config


def validate_config(config: ConfigParser):
    """Validate the configuration"""
    validated = False
    try:
        # Open the config file
        config = open_config()

        # Get the variables
        url = config["DEFAULT"]["url"]
        folder = Path(config["DEFAULT"]["folder"])

        # Check validity of the url
        parsed_url = urlparse(url)
        if not parsed_url.scheme:
            url = "http://" + url

        print(f"Checking connection to {url}")
        response = requests.get(url, timeout=5)
        if not response.ok:
            print(f"URL {url} not responding")

        if not folder.exists():
            print(f"Folder specified in config '{folder}' not found")

        # Check validity of the connection type
        connection_type = config["DEFAULT"]["connection"]

        if connection_type not in ConnectionType.__members__:
            print(f"Invalid connection type: {connection_type}")

        # Check validity of download mode
        download_mode = config["DEFAULT"]["download"]

        if download_mode not in DownloadMode.__members__:
            print(f"Invalid download mode: {download_mode}")


        validated = True
    except KeyError as error:
        print(f"Invalid configuration, missing key: {error}")

    except FileNotFoundError as error:
        print(error)

    finally:
        if not validated:
            print("Config file not initialized correctly.")
            print("Please run 'merge-downloader init' first.")

    return validated


def create_epilog() -> str:
    """Create the epilog message"""

    msg = "Actual configuration:\n"

    try:
        config = open_config()

        # print keys and values within config['DEFAULT']
        for key, value in config["DEFAULT"].items():
            msg += f"{key}={value}\n"

    except FileNotFoundError as error:
        msg += error

    return msg


def create_argparser() -> argparse.ArgumentParser:
    """Create the argument parser"""
    # First, create the MAIN parser
    parser = argparse.ArgumentParser(
        description="Merge Downloader",
        epilog=create_epilog(),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    #### Create a parent parser with DEFAULT args
    # these args (date and type) will be used for several other parsers
    default_args = argparse.ArgumentParser(add_help=False)
    default_args.add_argument(
        "-d",
        "--dates",
        nargs=2,
        type=DateProcessor.parse_date,
        help="Date range 'start_date end_date'. "
        "Dates can be specified any format that can be parsed e.g.: yyyymmdd, yyyy-mm, etc.",
    )
    default_args.add_argument(
        "-t",
        "--type",
        required=True,
        type=InpeTypes,
        help=f"Data type to download. It can be any of: {InpeTypes._member_names_}",  # pylint: disable=E1101, W0212
    )
    # ------------------------------------------

    ### Define subparsers ###
    subparsers = parser.add_subparsers()

    #### Define the RESET subcommand ####
    parser_reset = subparsers.add_parser(
        "reset",
        help="Clear configuration",
        description="The default configuration is saved to 'config.ini' file within"
        " the package's folder.",
    )
    parser_reset.set_defaults(func=reset)
    # ------------------------------------------

    #### Define the INIT subcommand ####
    parser_init = subparsers.add_parser(
        "init",
        help="Initialize the environment",
        description="The init command is used to set the FTP URL to pull the data from and the "
        " local download directory.",
    )
    parser_init.add_argument(
        "-f",
        "--folder",
        help="Local folder to download the files (relative path).",
        required=True,
        type=Path,
    )
    parser_init.add_argument(
        "-url",
        help=f"FTP URL of the server. Defaults to '{INPE_SERVER}'",
        default=INPE_SERVER,
    )
    parser_init.add_argument(
        "-c",
        "--connection",
        help=f"Connection type. Defaults to 'HTTP'"
        f"\nPossible values are {ConnectionType._member_names_}",  # pylint: disable=E1101, W0212
        default=ConnectionType.HTTP.value,
    )
    parser_init.add_argument(
        "-d",
        "--download",
        help=f"Download mode. Defaults to 'UPDATE'"
        f"\nPossible values are {DownloadMode._member_names_}",  # pylint: disable=E1101, W0212
        default=DownloadMode.UPDATE.value,
    )
    parser_init.set_defaults(func=init)
    # ------------------------------------------

    #### Define the SERIES subcommand ####
    parser_series = subparsers.add_parser(
        "series",
        help="Calculate the rain and create a time-series for the specified period.",
        description="If a shapefile is provided the rain is evaluated within the polygons."
        " It averages the rain spatially in the region.",
        parents=[default_args],
    )
    parser_series.add_argument(
        "-s",
        "--shp",
        help="Shapefile with polygon to cut the raster. If not specified, averages for "
        "the whole region (South America)",
        type=Path,
    )
    parser_series.add_argument(
        "-f",
        "--file",
        required=True,
        help="Specifies the .csv file to save the series to",
        type=Path,
    )
    parser_series.add_argument(
        "--chart",
        action="store_true",
        default=False,
        help="Exports a chart (.png) with the series",
    )
    parser_series.add_argument(
        "--anim",
        action="store_true",
        default=False,
        help="Exports an animated GIF (.gif) with the series",
    )
    parser_series.set_defaults(func=series)
    # ------------------------------------------

    #### Define the DOWNLOAD subcommand ####
    parser_download = subparsers.add_parser(
        "download",
        help="Download raster data",
        description="The raster data is downloaded in the original format (i.e., .nc or .grib2)",
        parents=[default_args],
    )
    parser_download.set_defaults(func=download)
    # ------------------------------------------

    return parser


# -------------------- General Functions --------------------
def create_downloader() -> Downloader:
    """Create a Downloader instance"""

    config = open_config()
    if validate_config(config):

        fd = FileDownloader(
            server=config["DEFAULT"]["url"],
            connection_type=ConnectionType[config["DEFAULT"]["connection"]],
            download_mode=DownloadMode[config["DEFAULT"]["download"]]
        )

        downloader = Downloader(
            file_downloader=fd,
            parsers=InpeParsers,
            local_folder=config["DEFAULT"]["folder"],
        )

        return downloader
    else:
        raise ValueError(
            "Config file not initialized correctly. Run `merge-downloader init` first"
        )


# -------------------- Commands Functions --------------------
def reset(_):
    """
    Clear (delete) default configuration.
    """
    file = config_file()

    if file.exists():
        print(f"Deleting file {file}")
        file.unlink()

    else:
        print("No default config to reset")


def init(args):
    """Initialize the package"""
    print("Initializing...")

    # setup the folder
    folder = args.folder.absolute()
    folder.mkdir(parents=False, exist_ok=True)
    print(f"Setting downloading folder to '{folder}'")

    config = ConfigParser()
    config["DEFAULT"] = {
        "url": args.url,
        "folder": folder.as_posix(),
        "connection": args.connection,
        "download": args.download,
    }

    with open(config_file(), "w", encoding="utf-8") as configfile:
        config.write(configfile)


def series(args):
    """Download a series"""

    dates = list(map(DateProcessor.pretty_date, args.dates))
    print(f"Downloading {args.type} series for range: {dates}")

    # first, create the cube
    downloader = create_downloader()
    cube = downloader.create_cube(
        start_date=args.dates[0],
        end_date=args.dates[1],
        datatype=args.type,
    )

    # Creating time-series
    if not args.shp:
        print("No shapefile provided. Evaluating rain for whole region (South America)")
        series_xr = cube.mean(dim=["latitude", "longitude"])
        series_pd = series_xr.to_series()
        shp = None

    else:
        shp = gpd.read_file(args.shp)

        print(f"Cutting raster to: {args.shp}")
        series_pd = GISUtil.get_time_series(
            cube=cube, shp=shp, reducer=xr.DataArray.mean
        )

    # Now we have the series in a Pandas Series object
    # save to file
    series_pd.to_csv(args.file.with_suffix(".csv"))
    print(f"Series exported to: {args.file.with_suffix('.csv')}")

    # if chart flag is True, plot a bar chart
    if args.chart:
        # create a thumbnail
        print(f"Creating bar chart: {args.file.with_suffix('.png')}")
        ChartUtil.save_bar(
            series=series_pd, datatype=args.type, filename=args.file.with_suffix(".png")
        )

    # if animation flag is True, create a gif file
    if args.anim:
        print(f"Creating animation file: {args.file.with_suffix('.gif')}")
        GISUtil.animate_cube(cube=cube, shp=shp, filename=args.file.with_suffix(".gif"))


def download(args):
    """Download merge/climatologic files"""

    dates = list(map(DateProcessor.pretty_date, args.dates))
    print(f"Downloading {args.type} series for range: {dates}")

    downloader = create_downloader()

    files = downloader.get_range(
        start_date=args.dates[0],
        end_date=args.dates[1],
        datatype=args.type,
    )

    print("The following files are available:")
    for file in files:
        print(file)


# -------------------- Main Entrypoint --------------------
def main():
    """Main entry point for the CLI"""
    parser = create_argparser()

    # Parse the arguments
    args = parser.parse_args()

    # Call the appropriate function
    # The 'func' attribute contains a reference to the function to be called
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
