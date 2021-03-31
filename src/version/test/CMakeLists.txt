# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        http_status_test
    SOURCES
        StatusHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:common_http_client_obj>
        $<TARGET_OBJECTS:common_ws_obj>
        $<TARGET_OBJECTS:common_ws_common_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_process_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_version_obj>
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)
