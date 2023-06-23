"""Setup module"""

from setuptools import setup, find_packages
from mergedownloader import __version__ as version

setup(
    name="merge-downloader",
    version=version,
    description="Downloader Package for rain obtained from MERGE/GPM model processed by INPE",
    author="Mauricio Cordeiro",
    author_email="cordmaur@gmail.com",
    packages=find_packages(),
    entry_points={"console_scripts": ["merge-downloader = mergedownloader.cli:main"]}
    # install_requires=[
    #     "geopandas",
    #     "xarray",
    #     "rasterio",
    #     "rioxarray",
    #     "cfgrib",
    #     "contextily",
    #     "ecCodes",
    #     "ecmwflibs",
    # ],
)
