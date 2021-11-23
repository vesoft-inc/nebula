nebula_add_executable(
    NAME
        storage_perf
    SOURCES
        StoragePerfTool.cpp
    OBJECTS
        $<TARGET_OBJECTS:graph_storage_client_obj>
        ${tools_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        gtest
)

#install(
#    TARGETS
#        storage_perf
#    DESTINATION
#        bin
#    COMPONENT
#        tool
#)

nebula_add_executable(
    NAME
        storage_integrity
    SOURCES
        StorageIntegrityTool.cpp
    OBJECTS
        $<TARGET_OBJECTS:graph_storage_client_obj>
        ${tools_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        gtest
)

#install(
#    TARGETS
#        storage_integrity
#    DESTINATION
#        bin
#    COMPONENT
#        tool
#)
