#!/bin/bash
# set -x
set -e

#
#  Copyright 2020-2025 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos
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
    echo " $0  -m <mpicc path> -l <libfabric path> -i <Install path> -s <Source path> -d <dmtcp path> -u <fuse3 path>"
    echo " Where:"
    echo " * <mpicc   path> = full path where the mpicc is installed."
    echo " * <libfabric   path> = full path where the libfabric is installed."
    echo " * <Install path> = full path where XPN is going to be installed."
    echo " * <Source  path> = full path to the source code XPN."
    echo " * <dmtcp  path> = full path to the source code of DMTCP."
    echo ""
}

LIBFABRIC_PATH=""
LIBFUSE3_ARGS=""
## get arguments
while getopts "m:f:u:i:s:d:" opt; do
    case "${opt}" in
          m) MPICC_PATH="-D ENABLE_MPI_SERVER=${OPTARG}"
             ;;
          f) LIBFABRIC_PATH=${OPTARG}
             ;;
          u) LIBFUSE3_ARGS=${OPTARG}
             ;;
          i) INSTALL_PATH=${OPTARG}
             ;;
          s) SRC_PATH=${OPTARG}
             ;;
          d) DMTCP_PATH="-D DMTCP_PATH=${OPTARG}"
             ;;
          *) echo " Error:"
             echo " * Unknown option: ${opt}"
             usage
             exit
             ;;
    esac
done

## check arguments
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
echo " * XPN libfuse: $LIBFUSE3_ARGS"
pushd .
cd "$SRC_PATH"
# rm -r build
mkdir -p build
cd build

GENERATOR="Unix Makefiles"
if command -v ninja &> /dev/null
then
   GENERATOR="Ninja"
fi

cmake -S .. -B . -D CMAKE_EXPORT_COMPILE_COMMANDS=1 -D BUILD_TESTS=ON -D CMAKE_INSTALL_PREFIX="${INSTALL_PATH}/xpn" $MPICC_PATH -D ENABLE_FABRIC_SERVER="${LIBFABRIC_PATH}" -G "${GENERATOR}" $DMTCP_PATH $LIBFUSE3_ARGS

cmake --build . -j "$(nproc)"

cmake --install .

popd
