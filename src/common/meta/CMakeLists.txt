# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    meta_obj SHARED
    GflagsManager.cpp
    SchemaProviderIf.cpp
    NebulaSchemaProvider.cpp
    SchemaManager.cpp
    ServerBasedSchemaManager.cpp
    IndexManager.cpp
    ServerBasedIndexManager.cpp
)
target_link_libraries(meta_obj PRIVATE conf_obj)
