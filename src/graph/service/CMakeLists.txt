# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    graph_flags_obj SHARED
    GraphFlags.cpp
)

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

nebula_add_subdirectory(test)
