FROM cordmaur/merge-downloader:v1
# FROM cordmaur/reporter_dev:alpine-v2

# RUN mkdir /workspaces

# Use an official GDAL image as the base image
# FROM osgeo/gdal:ubuntu-small-latest

# # install pip
# RUN apt-get update && apt-get -y install python3-pip --fix-missing
# RUN apt-get -y install git
# RUN apt-get -y install libeccodes0 libeccodes-dev
# RUN pip install geopandas rioxarray matplotlib notebook ipykernel contextily \
#     netCDF4 cfgrib eccodes adjustText pytest --no-cache-dir

# FROM ubuntu:20.04
# ENV    CPLUS_INCLUDE_PATH=/usr/include/gdal
# ENV    C_INCLUDE_PATH=/usr/include/gdal
# RUN    apt-get update 
# RUN    apt-get install -y software-properties-common 
# RUN    add-apt-repository ppa:deadsnakes/ppa 
# RUN    add-apt-repository ppa:ubuntugis/ppa
# RUN    apt-get update 
# RUN    apt-get install -y git
# RUN    apt-get install -y curl
# RUN    apt-get install -y python3.8 
# RUN    apt-get install -y python3-distutils python3-apt
# RUN    apt-get install -y python3-pip 
# RUN    apt-get install -y gdal-bin
# RUN    apt-get install -y libgdal-dev
# RUN    export CPLUS_INCLUDE_PATH
# RUN    export C_INCLUDE_PATH
# RUN    pip install GDAL
# COPY requirements.txt .

# Install the necessary dependencies
# RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install \
#     shapely \
#     fiona \
#     pyproj \
#     geopandas \
#     rioxarray \
#     pystac-client \
#     matplotlib \
#     notebook \
#     ipykernel

# RUN python3 -m pip install -U matplotlib


# FROM cordmaur/geospatial_minimal:latest

# # install git
# RUN apt-get update && apt-get -y install git && rm -rf /var/lib/apt/lists/*

# # install notebook and ipykernel
# RUN pip3 install --no-cache-dir notebook ipykernel

# EXPOSE 8888

# Copy the requirements.txt file to the container
# COPY requirements.txt ./