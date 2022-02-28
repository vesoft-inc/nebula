# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    hdfs_helper_obj SHARED
    HdfsCommandHelper.cpp
)

target_link_libraries(hdfs_helper_obj PUBLIC "${Glog_LIBRARY}" process_obj  )
nebula_add_subdirectory(test)
