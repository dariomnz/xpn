#!/bin/bash
# shellcheck disable=all
#set -x

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

# 2) working path...
#MPICC_PATH=$HOME/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/openmpi-4.1.3-4bpvwm3lcbftmjki6en35c4i5od6wjbr/bin/mpicc
#MPICC_PATH=$HOME/opt/spack/linux-ubuntu20.04-zen/gcc-9.4.0/mpich-4.0.2-a76rmlxbneoqdvemzjsyewp2akiiuxlj/bin/mpicc
# MPICC_PATH=$HOME/dariomnz/bin/mpich/bin/mpicc
MPICC_PATH=$HOME/dariomnz/bin/mpich-ch4-fabric/bin
FABRIC_PATH=/opt/libfabric
FABRIC_PATH=$HOME/dariomnz/bin/libfabric-2.0.0
INSTALL_PATH=$HOME/dariomnz/bin/
BASE_PATH=$(dirname $0)

spack load gcc@13.1.0

export CXX="/beegfs/home/javier.garciablas/spack/opt/spack/linux-ubuntu20.04-broadwell/gcc-9.4.0/gcc-13.1.0-5csi3fwipfhhwrxomd2ilsxkx4zzpy5h/bin/g++"
export LD_LIBRARY_PATH=$HOME/dariomnz/bin/mpich-ch4-fabric/lib:$HOME/dariomnz/bin/libfabric-2.0.0/lib:$LD_LIBRARY_PATH
export PATH=$HOME/dariomnz/bin/mpich-ch4-fabric/bin:$PATH

# 3) preconfigure build-me...
$BASE_PATH/../software/xpn.sh         -m $MPICC_PATH -f $FABRIC_PATH -i $INSTALL_PATH -s $BASE_PATH/../../../../xpn
# $BASE_PATH/../software/ior.sh         -m $MPICC_PATH/mpicc -i $INSTALL_PATH -s $BASE_PATH/../../../../ior
# $BASE_PATH/../software/lz4.sh         -m $MPICC_PATH -i $INSTALL_PATH -s $BASE_PATH/../../../../io500/build/pfind/lz4/
# $BASE_PATH/../software/io500.sh       -m $MPICC_PATH/mpicc -i $INSTALL_PATH -s $BASE_PATH/../../../../io500
