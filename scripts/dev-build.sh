
set -x
set -e

# intended usage from xpn root dir
# time ./scripts/dev-build.sh $(pwd)/build $(pwd)/install
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <build_directory> <install_directory>"
    exit 1
fi

cd $1

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$2/lib

cmake -S .. -B . \
    -D CMAKE_EXPORT_COMPILE_COMMANDS=1 \
    -D BUILD_TESTS=true \
    -D CMAKE_INSTALL_PREFIX=$2 \
    -D CMAKE_C_COMPILER=gcc \
    -D CMAKE_CXX_COMPILER=g++ \
    -D ENABLE_MPI_SERVER=on \
    -D MPI_INCLUDE_DIR=/usr/include/x86_64-linux-gnu/mpich \
    -D ENABLE_FABRIC_SERVER=on \
    -D ENABLE_FUSE=on \
    -D ENABLE_MQTT_SERVER=off

# cmake -S .. -B . \
#     -D BUILD_TESTS=true \
#     -D CMAKE_INSTALL_PREFIX=$2 \
#     -D CMAKE_C_COMPILER=gcc \
#     -D CMAKE_CXX_COMPILER=g++ \
#     -D ENABLE_MPI_SERVER=off \
#     -D ENABLE_FABRIC_SERVER=off \
#     -D ENABLE_MQ_SERVER=/usr/lib/x86_64-linux-gnu

# cmake -S .. -B . \
#     -D BUILD_TESTS=true \
#     -D CMAKE_INSTALL_PREFIX=$2 \
#     -D CMAKE_C_COMPILER=gcc \
#     -D CMAKE_CXX_COMPILER=g++ \
#     -D ENABLE_MPI_SERVER=off \
#     -D ENABLE_FABRIC_SERVER=/home/lab/bin/libfabric \
#     -D ENABLE_MQ_SERVER=/usr/lib/x86_64-linux-gnu

# cmake -S .. -B . \
#     -D BUILD_TESTS=true \
#     -D CMAKE_INSTALL_PREFIX=$2 \
#     -D CMAKE_C_COMPILER=gcc \
#     -D CMAKE_CXX_COMPILER=g++ \
#     -D ENABLE_MPI_SERVER=/usr/lib/x86_64-linux-gnu/mpich \
#     -D ENABLE_FABRIC_SERVER=/home/lab/bin/libfabric \
#     -D ENABLE_MQ_SERVER=/usr/lib/x86_64-linux-gnu

# cmake -S .. -B . \
#     -D BUILD_TESTS=true \
#     -D CMAKE_INSTALL_PREFIX=$2 \
#     -D CMAKE_C_COMPILER=mpicc \
#     -D CMAKE_CXX_COMPILER=mpic++ \
#     -D ENABLE_MPI_SERVER=/usr/lib/x86_64-linux-gnu/mpich \
#     -D ENABLE_FABRIC_SERVER=off \
#     -D ENABLE_MQ_SERVER=/usr/lib/x86_64-linux-gnu
cmake --build . -j $(nproc)

cmake --install .

ctest