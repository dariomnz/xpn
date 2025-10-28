if(ENABLE_MPI_SERVER)
    link_libraries("${MPI_LIBRARY}")
    include_directories("${MPI_INCLUDE_DIR}")
endif()

add_library(xpn_bypass SHARED "xpn_bypass.cpp")

set_target_properties(xpn_bypass PROPERTIES PREFIX "")

target_link_libraries(xpn_bypass PRIVATE xpn_static pthread dl )

target_include_directories(xpn_bypass PRIVATE 
    "${PROJECT_SOURCE_DIR}/src"
)

install(TARGETS xpn_bypass LIBRARY DESTINATION lib)
    
if(DMTCP_PATH)
    message(STATUS "Building DMTCP pluging")
    set(DMTCP_INCLUDE "${DMTCP_PATH}/include")
    set(DMTCP_PLUGIN "xpn_dmtcp")
    set(LIB_SOURCES "xpn_dmtcp.cpp" "xpn_dmtcp.hpp" "xpn_bypass_dmtcp.cpp")
    add_library(${DMTCP_PLUGIN} SHARED ${LIB_SOURCES})
    set_target_properties(${DMTCP_PLUGIN} PROPERTIES PREFIX "")
    target_include_directories(${DMTCP_PLUGIN} PUBLIC 
        "${PROJECT_SOURCE_DIR}/src"
        "${PROJECT_SOURCE_DIR}/src/xpn_client"
        ${DMTCP_INCLUDE} 
    )
    target_compile_definitions(${DMTCP_PLUGIN} PUBLIC DMTCP) # -DDMTCP
    target_link_libraries(${DMTCP_PLUGIN} PUBLIC rt xpn_static pthread dl)

    install(TARGETS ${DMTCP_PLUGIN} LIBRARY DESTINATION lib)

    set(DMTCP_PLUGIN "lfi_dmtcp")
    set(LIB_SOURCES "lfi_bypass.cpp")
    add_library(${DMTCP_PLUGIN} SHARED ${LIB_SOURCES})
    set_target_properties(${DMTCP_PLUGIN} PROPERTIES PREFIX "")
    target_include_directories(${DMTCP_PLUGIN} PUBLIC 
        "${PROJECT_SOURCE_DIR}/src"
        "${PROJECT_SOURCE_DIR}/src/xpn_client"
        "${PROJECT_SOURCE_DIR}/libs/lfi/include"
        ${DMTCP_INCLUDE}
        "${DMTCP_PATH}/../mpi-proxy-split/lower-half"
    )
    target_compile_definitions(${DMTCP_PLUGIN} PUBLIC DMTCP) # -DDMTCP
    target_link_libraries(${DMTCP_PLUGIN} PUBLIC rt xpn_static pthread dl)

    install(TARGETS ${DMTCP_PLUGIN} LIBRARY DESTINATION lib)

    
    add_library(xpn_bypass_debug SHARED "xpn_bypass_debug.cpp")

    set_target_properties(xpn_bypass_debug PROPERTIES PREFIX "")

    target_link_libraries(xpn_bypass_debug PRIVATE rt pthread dl )

    target_include_directories(xpn_bypass_debug PRIVATE 
        "${PROJECT_SOURCE_DIR}/src"
        "${PROJECT_SOURCE_DIR}/src/xpn_client"
        ${DMTCP_INCLUDE}
        "${DMTCP_PATH}/../mpi-proxy-split/lower-half"
    )

    install(TARGETS xpn_bypass_debug LIBRARY DESTINATION lib)
    
endif(DMTCP_PATH)