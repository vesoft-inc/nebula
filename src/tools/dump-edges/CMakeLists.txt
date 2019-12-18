nebula_add_executable(
    NAME
        dump_edges
    SOURCES
        DumpEdgesTool.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        glog
        gflags
)

