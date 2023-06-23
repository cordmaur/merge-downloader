from pathlib import Path

import geopandas as gpd
import contextily as cx

from mergedownloader.inpe import INPE
from mergedownloader.downloader import Downloader
from mergedownloader.utils import FileType
from rainreporter.reporter import RainReporter

import rasterio as rio
from mergedownloader.utils import GISUtil

reporter = RainReporter(
    server=INPE.FTPurl, remote_folder=INPE.DailyMERGEroot, download_folder="./tmp"
)


shapes = Path(
    "/Users/cordmaur/Library/CloudStorage/OneDrive-AgênciaNacionaldeÁguas/Trabalho/SOE/COVEC/Bases/bacias de interesse SOE"
)
assert shapes.exists()

basins = {
    file.stem.split("_")[-1]: file for file in shapes.iterdir() if file.suffix == ".shp"
}

ax, rain, shp = reporter.rain_report("20230101", "20230101", basins["ANA"])
ax[0].figure.savefig("figure1.png")

# plot the shape
plt_ax = shp.plot(edgecolor="white", alpha=0.1)
xmin, xmax, ymin, ymax = plt_ax.get_xlim() + plt_ax.get_ylim()

rain_basins = rain.sel(x=slice(xmin, xmax), y=slice(ymin, ymax))

# create a memory file and use it to create a memory dataset
profile = GISUtil.profile_from_xarray(rain_basins)

with rio.MemoryFile() as memfile:
    with memfile.open(**profile) as memdset:
        # write the data to the newly created dataset
        memdset.write(rain_basins)

    # with the dataset in memory, add the basemap
    cx.add_basemap(plt_ax, source=memfile, reset_extent=False, vmin=0, vmax=100)

    # now, let's create a colorbar for this

plt_ax.figure.savefig("figure2.png")
