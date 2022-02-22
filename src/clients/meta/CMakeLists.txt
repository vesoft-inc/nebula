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

nebula_add_subdirectory(stats)
nebula_add_subdirectory(test)
