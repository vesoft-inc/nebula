# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        http_client_test
    SOURCES
        HttpClientTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:fake_http_server_obj>
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        gtest
        curl
        gtest_main
)
