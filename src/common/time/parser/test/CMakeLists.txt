# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        datetime_parser_test
    SOURCES
        DateTimeParserTest.cpp
)
target_link_libraries(
        datetime_parser_test
        PRIVATE
        time_utils_obj
        thread_obj
        datatypes_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
        base_obj
        fs_obj
        datetime_parser_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
        
)
