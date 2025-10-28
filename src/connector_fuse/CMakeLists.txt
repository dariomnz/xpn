add_executable(fuse-xpn "fuse-xpn.cpp")

target_link_libraries(fuse-xpn PRIVATE xpn "${FUSE_LIBRARY}")

target_link_directories(fuse-xpn PRIVATE "${FUSE_LIBRARY}")

target_include_directories(fuse-xpn PRIVATE "${FUSE_INCLUDE_DIR}")

target_compile_options(fuse-xpn PRIVATE "-D_FILE_OFFSET_BITS=64")

install(TARGETS fuse-xpn RUNTIME DESTINATION bin)