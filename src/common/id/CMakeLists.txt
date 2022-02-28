# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    snowflake_obj SHARED
    Snowflake.cpp
)

nebula_add_library(
    uuid_obj SHARED
    UUID.cpp
)
target_link_libraries(snowflake_obj PUBLIC ${Glog_LIBRARY})

nebula_add_subdirectory(test)
