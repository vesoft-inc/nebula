# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_executable(
    NAME
        storage_perf
    SOURCES
        StoragePerfTool.cpp
    OBJECTS
        ${tools_test_deps}
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
        ${tools_test_deps}
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
