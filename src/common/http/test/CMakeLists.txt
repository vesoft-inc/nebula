# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        http_client_test
    SOURCES
        HttpClientTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:fake_http_server_obj>
    LIBRARIES
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        curl
        ${PROXYGEN_LIBRARIES}
)
