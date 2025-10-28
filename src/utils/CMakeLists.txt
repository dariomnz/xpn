
set(UTILS_LIST_CPP
	# xpn_rebuild_active_reader # TODO: fix with metadata
	xpn_server_stats
	xpn_server_monitor
	xpn_server_flush
	xpn_server_preload
)

if(ENABLE_MPI_SERVER)
	list(APPEND UTILS_LIST_CPP 
		xpn_flush
		xpn_preload
		xpn_expand
		xpn_shrink
	)
	
    link_libraries("${MPI_LIBRARY}")
    include_directories("${MPI_INCLUDE_DIR}")
endif()


foreach (util ${UTILS_LIST_CPP})
	add_executable(${util} ${util}.cpp)
	target_link_libraries(${util} PRIVATE xpn)
	install(TARGETS ${util} RUNTIME DESTINATION bin)
endforeach (util)


set(UTILS_LIST_C
	xpn_get_block_locality
	xpn_tree
)

foreach (util ${UTILS_LIST_C})
	add_executable(${util} ${util}.c)
	target_link_libraries(${util} PRIVATE xpn)
	install(TARGETS ${util} RUNTIME DESTINATION bin)
endforeach (util)

set(UTILS_BASH
	xpn_ckpt
)

foreach (util ${UTILS_BASH})
	install(PROGRAMS ${util} DESTINATION bin)
endforeach (util)