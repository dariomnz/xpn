
file(GLOB XPN_FILESYSTEM_HEADERS
    "*.hpp"
)

file(GLOB XPN_FILESYSTEM_SOURCE
    "*.cpp"
)

add_library(xpn_filesystem OBJECT ${XPN_FILESYSTEM_SOURCE} ${XPN_FILESYSTEM_HEADERS})
target_link_libraries(xpn_filesystem xpn_base_cpp)
target_include_directories(xpn_filesystem PUBLIC
    "${PROJECT_SOURCE_DIR}/src"
    "${PROJECT_SOURCE_DIR}/src/xpn_client"
)   