
file(GLOB XPN_NFI_HEADERS
	"*.hpp"
	"nfi_local/*.hpp"
	"nfi_xpn_server/*.hpp"
	)
	
file(GLOB XPN_NFI_SOURCE
	"*.cpp"
	"nfi_local/*.cpp"
	"nfi_xpn_server/*.cpp"
)

add_library(xpn_nfi OBJECT ${XPN_NFI_HEADERS} ${XPN_NFI_SOURCE})
target_link_libraries(xpn_nfi PRIVATE xpn_base_cpp lz4)

target_include_directories(xpn_nfi PRIVATE
	"${PROJECT_SOURCE_DIR}/src"
	"${PROJECT_SOURCE_DIR}/src/xpn_client"
)

add_subdirectory(nfi_sck_server)
add_subdirectory(nfi_mpi_server)
add_subdirectory(nfi_fabric_server)
add_subdirectory(nfi_mqtt_server)