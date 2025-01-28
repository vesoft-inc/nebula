# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        embedding_client_test
    SOURCES
        EmbeddingClientTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:embedding_client_obj>
        $<TARGET_OBJECTS:conf_obj>
    LIBRARIES
        gtest
        gtest_main
        gmock
        curl
        ${PROXYGEN_LIBRARIES}
        gflags
) 