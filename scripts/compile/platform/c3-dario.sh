#!/bin/bash
# shellcheck disable=all
# set -x

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


# 1) software (if needed)...
#spack load openmpi
#spack load mpich
#spack load pkg-config
module load cmake mpich
spack load autoconf m4

# 2) working path...
MPICC_PATH=/opt/ohpc/pub/mpi/mpich-4.3.0-ofi
FABRIC_PATH=$HOME/bin/libfabric
INSTALL_PATH=$HOME/bin/
BASE_PATH=$(dirname $0)

export LD_LIBRARY_PATH=$MPICC_PATH/lib:$FABRIC_PATH/lib:$LD_LIBRARY_PATH
export PATH=$MPICC_PATH/bin:$PATH

# 3) preconfigure build-me...
$BASE_PATH/../software/xpn.sh         -m $MPICC_PATH -f $FABRIC_PATH -i $INSTALL_PATH -s $BASE_PATH/../../../../xpn
# $BASE_PATH/../software/ior.sh         -m $MPICC_PATH/bin/mpicc -i $INSTALL_PATH -s $BASE_PATH/../../../../ior
# $BASE_PATH/../software/lz4.sh         -m $MPICC_PATH/bin/mpicc -i $INSTALL_PATH -s $BASE_PATH/../../../../io500/build/pfind/lz4/
# $BASE_PATH/../software/io500.sh       -m $MPICC_PATH/bin/mpicc -i $INSTALL_PATH -s $BASE_PATH/../../../../io500
