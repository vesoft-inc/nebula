# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    mock_obj OBJECT
    MockCluster.cpp
    MockData.cpp
    AdHocIndexManager.cpp
    AdHocSchemaManager.cpp
)


nebula_add_library(
    fake_http_server_obj OBJECT
    FakeHttpServer.cpp
)

# nebula_add_executable(
#     NAME fake_http_server 
#     SOURCES main.cpp
#     OBJECTS $<TARGET_OBJECTS:fake_http_server_obj>
#     LIBRARIES ${PROXYGEN_LIBRARIES}
# )

