
file(GLOB_RECURSE xpn_server_co_HEADERS 
    "*.hpp"
)

file(GLOB_RECURSE xpn_server_co_SOURCE 
    "*.cpp"
)

# add_compile_options("-DDEBUG")
add_executable(xpn_server_co ${xpn_server_co_SOURCE})

target_link_libraries(xpn_server_co PRIVATE xpn_base_cpp lfi rt)

target_include_directories(xpn_server_co PRIVATE
    "${PROJECT_SOURCE_DIR}/src"
    "${PROJECT_SOURCE_DIR}/src/xpn_client"
    "${PROJECT_SOURCE_DIR}/src/xpn_server"
    "${PROJECT_SOURCE_DIR}/src/xpn_server_co"
)

install(TARGETS xpn_server_co RUNTIME DESTINATION bin)