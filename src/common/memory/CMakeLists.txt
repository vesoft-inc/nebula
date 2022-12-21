# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
if (ENABLE_ASAN)
    nebula_add_library(
            memory_obj OBJECT
            MemoryUtils.cpp
            MemoryTracker.cpp
    )
else ()
    nebula_add_library(
            memory_obj OBJECT
            MemoryUtils.cpp
            MemoryTracker.cpp
            NewDelete.cpp
    )
endif ()

nebula_add_subdirectory(test)
