# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        response_encode_decode_test
    SOURCES
        ResponseEncodeDecodeTest.cpp
)
target_link_libraries(
        response_encode_decode_test
        PRIVATE
        base_obj
        graph_obj
        graph_thrift_obj
        common_thrift_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
        datatypes_obj
        wkt_wkb_io_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
