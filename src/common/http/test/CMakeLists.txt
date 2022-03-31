# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        http_client_test
    SOURCES
        HttpClientTest.cpp
)
target_link_libraries(
        http_client_test
        PRIVATE
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
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
        gtest
        gtest_main
        ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
