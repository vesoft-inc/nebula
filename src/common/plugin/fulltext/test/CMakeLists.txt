# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

# nebula_add_test(
#     NAME
#         es_test
#     SOURCES
#         ElasticsearchTest.cpp
#     OBJECTS
#         $<TARGET_OBJECTS:base_obj>
#         $<TARGET_OBJECTS:http_client_obj>
#         $<TARGET_OBJECTS:es_adapter_obj>
#     LIBRARIES
#         gtest
#         gtest_main
#         gmock
#         curl
#         ${PROXYGEN_LIBRARIES}
# )
