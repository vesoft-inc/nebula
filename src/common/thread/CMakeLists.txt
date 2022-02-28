# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    thread_obj SHARED
    NamedThread.cpp
    GenericWorker.cpp
    GenericThreadPool.cpp
)
target_link_libraries(thread_obj PUBLIC "${Glog_LIBRARY}"  )

nebula_add_subdirectory(test)
