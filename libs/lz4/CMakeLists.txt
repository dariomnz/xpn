
cmake_minimum_required(VERSION 3.16)

project(LZ4 VERSION 1.10.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)

# At the moment we only use the block compression
set(LZ4_HEADERS
	"lz4.h"
)

set(LZ4_SOURCE
	"lz4.c"
)

# file(GLOB_RECURSE LZ4_HEADERS
# 	"*.h"
# )

# file(GLOB_RECURSE LZ4_SOURCE
# 	"*.c"
# )

add_library(lz4 STATIC ${LZ4_HEADERS} ${LZ4_SOURCE})

target_include_directories(lz4 PUBLIC
	"${PROJECT_SOURCE_DIR}"
)