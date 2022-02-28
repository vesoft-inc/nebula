# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    thrift_obj SHARED
    ThriftClientManager.cpp
)
target_link_libraries(thrift_obj PUBLIC ${Glog_LIBRARY})

