# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    meta_client_obj SHARED
    MetaClient.cpp
)

nebula_add_library(
    file_based_cluster_id_man_obj SHARED
    FileBasedClusterIdMan.cpp
)
target_link_libraries(file_based_cluster_id_man_obj PUBLIC ${Glog_LIBRARY})
nebula_add_subdirectory(stats)
target_link_libraries(meta_client_obj PRIVATE meta_obj http_client_obj ws_common_obj thrift_obj meta_client_stats_obj)
nebula_add_subdirectory(test)
