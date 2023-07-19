# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    storage_client_obj OBJECT
    StorageClient.cpp
)


nebula_add_library(
    storage_client_base_obj OBJECT
    StorageClientBase.cpp
)


nebula_add_library(
    internal_storage_client_obj OBJECT
    InternalStorageClient.cpp
)

nebula_add_subdirectory(stats)

