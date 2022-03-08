# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    ssl_obj
    SHARED
    SSLConfig.cpp
)

target_link_libraries(ssl_obj PUBLIC folly glog)
