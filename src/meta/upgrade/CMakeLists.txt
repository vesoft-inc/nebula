# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    meta_data_upgrade_obj OBJECT
    MetaDataUpgrade.cpp
    v2/MetaServiceUtilsV2.cpp
)

add_dependencies(
    meta_data_upgrade_obj
    meta_v2_thrift_obj
)

nebula_add_subdirectory(v2)
