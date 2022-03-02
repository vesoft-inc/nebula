# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    storage_client_obj SHARED
    StorageClient.cpp
)


nebula_add_library(
    storage_client_base_obj SHARED
    StorageClientBase.cpp
)
target_link_libraries(storage_client_base_obj PUBLIC "${Glog_LIBRARY}")
target_link_libraries(storage_client_obj PRIVATE storage_client_base_obj "${Glog_LIBRARY}" storage_client_stats_obj thrift_obj )


nebula_add_library(
    internal_storage_client_obj SHARED
    InternalStorageClient.cpp
)
target_link_libraries(internal_storage_client_obj PUBLIC storage_client_stats_obj "${Glog_LIBRARY}")

nebula_add_subdirectory(stats)

