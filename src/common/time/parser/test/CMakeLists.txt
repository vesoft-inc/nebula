# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        datetime_parser_test
    SOURCES
        DateTimeParserTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:datetime_parser_obj>
    LIBRARIES
        gtest
)
