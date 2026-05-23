
set(TEST_LIST 
	open-read-close
	open-write-close
)

# add the executables
foreach (test ${TEST_LIST})
    add_executable(${test} ${test}.c)
	set_target_properties(${test} PROPERTIES LINKER_LANGUAGE C)
    target_link_libraries(${test} xpn)
    install(TARGETS ${test} RUNTIME DESTINATION test/performance/xpn)
endforeach (test)

set(TEST_LIST 
	open-read-close-thread
	open-write-close-thread
)

# add the executables
foreach (test ${TEST_LIST})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} xpn)
    install(TARGETS ${test} RUNTIME DESTINATION test/performance/xpn)
endforeach (test)

if (ENABLE_MPI_SERVER)
    link_libraries("${MPI_LIBRARY}")
    include_directories("${MPI_INCLUDE_DIR}")

    set(MPI_TEST "mpi-open-w-r-close")
    add_executable(${MPI_TEST} ${MPI_TEST}.cpp)
    target_link_libraries(${MPI_TEST} PRIVATE xpn)
    install(TARGETS ${MPI_TEST} RUNTIME DESTINATION test/performance/xpn)
endif()