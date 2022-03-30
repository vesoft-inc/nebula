nebula_add_executable(
    NAME
        meta_dump
    SOURCES
        MetaDumpTool.cpp
    OBJECTS
        ${tools_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        glog
        gflags
)

install(
    TARGETS
        meta_dump
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)
