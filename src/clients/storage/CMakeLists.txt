# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    graph_storage_client_obj OBJECT
    GraphStorageClient.cpp
)


nebula_add_library(
    general_storage_client_obj OBJECT
    GeneralStorageClient.cpp
)


nebula_add_library(
    storage_client_base_obj OBJECT
    StorageClientBase.cpp
)


nebula_add_library(
    internal_storage_client_obj OBJECT
    InternalStorageClient.cpp
)

