# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    hdfs_helper_obj OBJECT
    HdfsCommandHelper.cpp
)

add_dependencies(module_common
    hdfs_helper_obj)

nebula_add_subdirectory(test)
