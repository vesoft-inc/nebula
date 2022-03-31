# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    meta_obj SHARED
    GflagsManager.cpp
    SchemaProviderIf.cpp
    NebulaSchemaProvider.cpp
)
nebula_add_library(
    meta_manager_obj SHARED
    SchemaManager.cpp
    ServerBasedSchemaManager.cpp
    IndexManager.cpp
    ServerBasedIndexManager.cpp
)
target_link_libraries(meta_obj PRIVATE conf_obj)
target_link_libraries(meta_manager_obj PRIVATE meta_client_obj conf_obj)
