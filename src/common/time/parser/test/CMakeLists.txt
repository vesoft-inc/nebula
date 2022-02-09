# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

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
