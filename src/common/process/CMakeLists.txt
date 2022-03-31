# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    process_obj SHARED
    ProcessUtils.cpp
)
target_link_libraries(process_obj PUBLIC fs_obj  base_obj )

nebula_add_subdirectory(test)
