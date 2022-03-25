# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


nebula_add_library(
  memory_obj OBJECT
  MemoryUtils.cpp
)

nebula_add_subdirectory(test)
