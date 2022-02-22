# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        http_client_test
    SOURCES
        HttpClientTest.cpp
    LIBRARIES
        http_client_obj
        ws_obj
        ws_common_obj
        process_obj
        fs_obj
        stats_obj
        time_obj
        base_obj
        version_obj
        datatypes_obj
        wkt_wkb_io_obj
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        gtest
        gtest_main
)
