#!/bin/bash
set -e

#
#  Copyright 2020-2024 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos
#
#  This file is part of Expand.
#
#  Expand is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Expand is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Expand.  If not, see <http://www.gnu.org/licenses/>.
#


function usage {
    echo ""
    echo " Usage:"
    echo " $0  -m <mpicc path> -l <libfabric path> -i <Install path> -s <Source path>"
    echo " Where:"
    echo " * <mpicc   path> = full path where the mpicc is installed."
    echo " * <libfabric   path> = full path where the libfabric is installed."
    echo " * <Install path> = full path where XPN is going to be installed."
    echo " * <Source  path> = full path to the source code XPN."
    echo ""
}

LIBFABRIC_PATH=""
## get arguments
while getopts "m:f:i:s:" opt; do
    case "${opt}" in
          m) MPICC_PATH=${OPTARG}
             ;;
          f) LIBFABRIC_PATH=${OPTARG}
             ;;
          i) INSTALL_PATH=${OPTARG}
             ;;
          s) SRC_PATH=${OPTARG}
             ;;
          *) echo " Error:"
             echo " * Unknown option: ${opt}"
             usage
             exit
             ;;
    esac
done

## check arguments
if [ "$MPICC_PATH" == "" ]; then
   echo " Error:"
   echo " * Empty MPICC_PATH"
   usage
   exit
fi
if [ "$INSTALL_PATH" == "" ]; then
   echo " Error:"
   echo " * Empty INSTALL_PATH"
   usage
   exit
fi
if [ "$SRC_PATH" == "" ]; then
   echo " Error:"
   echo " * Empty SRC_PATH"
   usage
   exit
fi
if [ ! -d "$SRC_PATH" ]; then
   echo " Skip XPN:"
   echo " * Directory not found: $SRC_PATH"
   exit
fi


## XPN
echo " * XPN: preparing directories..."

rm -fr "${INSTALL_PATH}/xpn"

echo " * XPN: compiling and installing..."
echo " * XPN mpi: $MPICC_PATH"
echo " * XPN libfabric: $LIBFABRIC_PATH"
pushd .
cd "$SRC_PATH"
# rm -r build
mkdir -p build
cd build

cmake -S .. -B . -D BUILD_TESTS=ON -D CMAKE_INSTALL_PREFIX="${INSTALL_PATH}/xpn" -D CMAKE_C_COMPILER="${MPICC_PATH}"/mpicc -D CMAKE_CXX_COMPILER="${MPICC_PATH}"/mpicxx -D ENABLE_FABRIC_SERVER="${LIBFABRIC_PATH}"

cmake --build . -j "$(nproc)"

cmake --install .

popd
