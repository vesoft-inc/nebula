# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    conf_obj SHARED
    Configuration.cpp
)

target_link_libraries(conf_obj PUBLIC "${Glog_LIBRARY}"  )
nebula_add_subdirectory(test)
