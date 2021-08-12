# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    meta_data_upgrade_obj OBJECT
    MetaDataUpgrade.cpp
    v1/MetaServiceUtilsV1.cpp
)

add_dependencies(
    meta_data_upgrade_obj
    meta_v1_thrift_obj
)

nebula_add_subdirectory(v1)
