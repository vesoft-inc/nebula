# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    meta_data_upgrade_obj SHARED
    MetaDataUpgrade.cpp
    v2/MetaServiceUtilsV2.cpp
)

target_link_libraries(
    meta_data_upgrade_obj
    PUBLIC
    meta_v2_thrift_obj
)

nebula_add_subdirectory(v2)
