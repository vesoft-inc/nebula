# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        flags_access_test
    SOURCES
        FlagsAccessTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        gtest
)


nebula_add_test(
    NAME
        stats_read_test
    SOURCES
        StatsReaderTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        gtest
)


nebula_add_test(
    NAME
        router_test
    SOURCES
        RouterTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:ws_common_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        gtest
)

nebula_add_test(
    NAME
        http_status_test
    SOURCES
        StatusHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:ws_common_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:version_obj>
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        gtest
)
