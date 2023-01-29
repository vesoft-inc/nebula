# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        response_encode_decode_test
    SOURCES
        ResponseEncodeDecodeTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:graph_obj>
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:memory_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
    LIBRARIES
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
)
