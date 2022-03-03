# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    graph_flags_obj SHARED
    GraphFlags.cpp
)

target_link_libraries(graph_flags_obj PUBLIC "${Glog_LIBRARY}" version_obj)
nebula_add_library(
    service_obj SHARED
    GraphService.cpp
    GraphServer.cpp
)

nebula_add_library(
    query_engine_obj SHARED
    QueryEngine.cpp
    QueryInstance.cpp
)

nebula_add_library(
    graph_auth_obj SHARED
    PermissionManager.cpp
    PermissionCheck.cpp
    PasswordAuthenticator.cpp
    CloudAuthenticator.cpp
)
target_link_libraries(graph_auth_obj PUBLIC  glog)
target_link_libraries(service_obj PUBLIC meta_client_obj graph_session_obj graph_auth_obj)

nebula_add_subdirectory(test)
