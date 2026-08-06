#!/bin/bash
# MESMO SCRIPT EXECUTADO NO CLUSTER MAS SEM OS COMANDOS sudo
set -x

cd ~/

PROJ_VERSION="9.3.1"
GDAL_VERSION="3.8.1"

# Define GDAL paths
GDAL_LIB_PATH="/usr/lib64"
GDAL_BIN_PATH="/usr/bin"
GDAL_DATA_PATH="/usr/share/gdal"

# Install required dependencies
dnf -y install gcc-c++ cpp sqlite-devel libtiff cmake python3-pip \
    python-devel python3-devel python3-setuptools python3-wheel \
    openssl-devel tcl libtiff-devel \
    libcurl-devel swig libpng-devel libjpeg-turbo-devel expat-devel

# Install PROJ
wget https://download.osgeo.org/proj/proj-${PROJ_VERSION}.tar.gz
tar zxvf proj-${PROJ_VERSION}.tar.gz
cd proj-${PROJ_VERSION}/
mkdir build
cd build
cmake ..
cmake --build . --parallel $(nproc)
cmake --install . --prefix /usr
cd ~

# Install GDAL
wget https://github.com/OSGeo/gdal/releases/download/v${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz
tar xvzf gdal-${GDAL_VERSION}.tar.gz
cd gdal-${GDAL_VERSION}/
mkdir build
cd build
cmake -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF -DOGR_BUILD_OPTIONAL_DRIVERS=OFF ..
cmake --build . --parallel $(nproc)
cmake --install . --prefix /usr
cd ~

# Set GDAL environment variables
export LD_LIBRARY_PATH=${GDAL_LIB_PATH}:$LD_LIBRARY_PATH
export PATH=${GDAL_BIN_PATH}:$PATH
export GDAL_DATA=${GDAL_DATA_PATH}
