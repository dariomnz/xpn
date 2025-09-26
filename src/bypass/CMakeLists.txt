
if(ENABLE_MPI_SERVER)
    link_libraries("${MPI_LIBRARY}")
    include_directories("${MPI_INCLUDE_DIR}")
endif()

set(BYPASS_SOURCES
    xpn_bypass
    xpn_bypass_debug
)

foreach (file ${BYPASS_SOURCES})
    add_library(${file} SHARED "${file}.cpp")

    set_target_properties(${file} PROPERTIES PREFIX "")

    target_link_libraries(${file} PRIVATE xpn pthread dl )

    target_include_directories(${file} PRIVATE 
        "${PROJECT_SOURCE_DIR}/src"
    )

    install(TARGETS ${file} LIBRARY DESTINATION lib)
endforeach (file)

if(DMTCP_PATH)
    message(STATUS "Building DMTCP pluging")
    set(DMTCP_INCLUDE "${DMTCP_PATH}/include")
    set(DMTCP_PLUGIN "xpn_dmtcp")
    set(LIB_SOURCES "xpn_dmtcp.cpp" "xpn_bypass_dmtcp.cpp")
    add_library(${DMTCP_PLUGIN} SHARED ${LIB_SOURCES})
    set_target_properties(${DMTCP_PLUGIN} PROPERTIES PREFIX "")
    target_include_directories(${DMTCP_PLUGIN} PUBLIC 
        "${PROJECT_SOURCE_DIR}/src"
        "${PROJECT_SOURCE_DIR}/src/xpn_client"
        ${DMTCP_INCLUDE} 
    )
    target_compile_definitions(${DMTCP_PLUGIN} PUBLIC DMTCP) # -DDMTCP
    target_link_libraries(${DMTCP_PLUGIN} PUBLIC rt xpn pthread dl)

    install(TARGETS ${DMTCP_PLUGIN} LIBRARY DESTINATION lib)

    set(DMTCP_PLUGIN "lfi_dmtcp")
    set(LIB_SOURCES "lfi_bypass.cpp")
    add_library(${DMTCP_PLUGIN} SHARED ${LIB_SOURCES})
    set_target_properties(${DMTCP_PLUGIN} PROPERTIES PREFIX "")
    target_include_directories(${DMTCP_PLUGIN} PUBLIC 
        "${PROJECT_SOURCE_DIR}/src"
        "${PROJECT_SOURCE_DIR}/src/xpn_client"
        ${DMTCP_INCLUDE}
        "${DMTCP_PATH}/../mpi-proxy-split/lower-half"
    )
    target_compile_definitions(${DMTCP_PLUGIN} PUBLIC DMTCP) # -DDMTCP
    target_link_libraries(${DMTCP_PLUGIN} PUBLIC rt xpn pthread dl)

    install(TARGETS ${DMTCP_PLUGIN} LIBRARY DESTINATION lib)
    
endif(DMTCP_PATH)