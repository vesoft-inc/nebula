# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        response_encode_decode_test
    SOURCES
        ResponseEncodeDecodeTest.cpp
    LIBRARIES
        base_obj
        graph_obj
        graph_thrift_obj
        common_thrift_obj
        datatypes_obj
        wkt_wkb_io_obj
    LIBRARIES
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
)
