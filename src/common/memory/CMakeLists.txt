# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


nebula_add_library(
  memory_obj SHARED
  MemoryUtils.cpp
)
target_link_libraries(memory_obj PUBLIC "${Glog_LIBRARY}" base_obj  fs_obj )

nebula_add_subdirectory(test)
