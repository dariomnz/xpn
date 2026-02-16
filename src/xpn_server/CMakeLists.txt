
file(GLOB XPN_SERVER_HEADERS 
    "*.hpp"
    "filesystem/*.hpp"
)

file(GLOB XPN_SERVER_SOURCE 
    "*.cpp"
    "filesystem/*.cpp"
)

add_executable(xpn_server ${XPN_SERVER_HEADERS} ${XPN_SERVER_SOURCE})

target_link_libraries(xpn_server PRIVATE xpn lz4)

target_include_directories(xpn_server PRIVATE
    "${PROJECT_SOURCE_DIR}/src"
    "${PROJECT_SOURCE_DIR}/src/xpn_client"
)

add_subdirectory(fabric_server)
add_subdirectory(mpi_server)
add_subdirectory(mqtt_server)
add_subdirectory(sck_server)

add_custom_command(
    OUTPUT xpn_stop_server
    COMMAND ${CMAKE_COMMAND} -E create_symlink
            xpn_server
            xpn_stop_server
    DEPENDS xpn_server
    COMMENT "Create_symlink xpn_server -> xpn_stop_server"
)

add_custom_target(create_symlinks ALL
    DEPENDS
        xpn_stop_server
)

install(TARGETS xpn_server RUNTIME DESTINATION bin)

install(CODE "
    message(STATUS \"Create_symlink: from \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/xpn_server to \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/xpn_stop_server\")
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/xpn_server \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/xpn_stop_server)
")