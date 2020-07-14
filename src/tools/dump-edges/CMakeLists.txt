nebula_add_executable(
    NAME
        dump_edges
    SOURCES
        DumpEdgesTool.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:kvstore_storage_utils_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        glog
        gflags
)

