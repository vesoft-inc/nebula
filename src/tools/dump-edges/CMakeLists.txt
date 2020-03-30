nebula_add_executable(
    NAME
        dump_edges
    SOURCES
        DumpEdgesTool.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:meta_service_handler>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        glog
        gflags
)

