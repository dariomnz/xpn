
add_subdirectory(nfi)
add_subdirectory(xpn)

file(GLOB XPN_HEADERS
	"*.h"
)

file(GLOB XPN_SOURCE
	"*.cpp"
)

add_library(xpn SHARED ${XPN_HEADERS} ${XPN_SOURCE})
target_link_libraries(xpn PRIVATE xpn_base_cpp xpn_filesystem xpn_core xpn_nfi)

add_library(xpn_static STATIC $<TARGET_OBJECTS:xpn>)
set_target_properties(xpn_static PROPERTIES OUTPUT_NAME xpn)
target_link_libraries(xpn_static PRIVATE xpn_base_cpp xpn_filesystem xpn_core xpn_nfi)

target_include_directories(xpn PUBLIC
	"${PROJECT_SOURCE_DIR}/src"
	"${PROJECT_SOURCE_DIR}/src/xpn_client"
)
target_include_directories(xpn_static PUBLIC
	"${PROJECT_SOURCE_DIR}/src"
	"${PROJECT_SOURCE_DIR}/src/xpn_client"
)

install(TARGETS xpn LIBRARY DESTINATION lib)
install(TARGETS xpn_static LIBRARY DESTINATION lib)
install(FILES "xpn.h" DESTINATION include)
