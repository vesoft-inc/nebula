# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        datetime_parser_test
    SOURCES
        DateTimeParserTest.cpp
    LIBRARIES
        time_utils_obj
        thread_obj
        datatypes_obj
        base_obj
        fs_obj
        wkt_wkb_io_obj
        datetime_parser_obj
    LIBRARIES
        gtest
)
