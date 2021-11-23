nebula_add_executable(
    NAME
        simple_kv_verify
    SOURCES
        SimpleKVVerifyTool.cpp
    OBJECTS
        ${tools_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

#install(
#    TARGETS
#        simple_kv_verify
#    DESTINATION
#        bin
#    COMPONENT
#        tool
#)
