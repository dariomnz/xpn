
set(TESTS
    block-placement
    init-destroy
    write-read
    fwrite-fread
    readdir
)

foreach(TEST_NAME IN LISTS TESTS)

    add_executable(${TEST_NAME} ${TEST_NAME}.cpp)

    target_link_libraries(${TEST_NAME} PRIVATE xpn)

    add_test(
        NAME ${TEST_NAME} 
        COMMAND ${TEST_NAME} 
    )
    set_tests_properties(${TEST_NAME} PROPERTIES
        ENVIRONMENT "PATH=${CMAKE_BINARY_DIR}/src/xpn_server:$ENV{PATH}"
    )
endforeach()