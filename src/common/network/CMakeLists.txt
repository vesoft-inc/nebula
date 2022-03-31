# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    network_obj SHARED
    NetworkUtils.cpp
)
target_link_libraries(network_obj PUBLIC  base_obj fs_obj)

nebula_add_subdirectory(test)
