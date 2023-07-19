# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    snowflake_obj OBJECT
    Snowflake.cpp
)

nebula_add_library(
    uuid_obj OBJECT
    UUID.cpp
)

nebula_add_library(
    segment_id_obj OBJECT
    SegmentId.cpp
)

nebula_add_subdirectory(test)
