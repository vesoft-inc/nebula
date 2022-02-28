# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    fs_obj SHARED
    FileUtils.cpp
    TempDir.cpp
    TempFile.cpp
)

target_link_libraries(fs_obj PUBLIC "${Glog_LIBRARY}"  )
nebula_add_subdirectory(test)
