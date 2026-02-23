
set(TEST_LIST 
	test-performance-disk
)

# add the executables
foreach (test ${TEST_LIST})
    add_executable(${test} ${test}.c)
	set_target_properties(${test} PROPERTIES LINKER_LANGUAGE C)
    target_link_libraries(${test} pthread)
    install(TARGETS ${test} RUNTIME DESTINATION test/disk)
endforeach (test)

if(NOT ENABLE_MPI_SERVER)  
    return()
endif()

set(TEST_LIST 
	test-performance-disk-mpi
	test-performance-disk-interval
)

# add the executables
foreach (test ${TEST_LIST})
    add_executable(${test} ${test}.c)
	set_target_properties(${test} PROPERTIES LINKER_LANGUAGE C)
    target_link_libraries(${test} "${MPI_LIBRARY}")
    target_include_directories(${test} PUBLIC "${MPI_INCLUDE_DIR}")
    install(TARGETS ${test} RUNTIME DESTINATION test/disk)
endforeach (test)