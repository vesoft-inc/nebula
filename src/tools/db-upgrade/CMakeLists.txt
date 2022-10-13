# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_executable(
    NAME
        db_upgrader
    SOURCES
        DbUpgraderTool.cpp
        NebulaKeyUtilsV1.cpp
        NebulaKeyUtilsV2.cpp
        NebulaKeyUtilsV3.cpp
        DbUpgrader.cpp
    OBJECTS
        ${tools_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
)

install(
    TARGETS
        db_upgrader
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)
