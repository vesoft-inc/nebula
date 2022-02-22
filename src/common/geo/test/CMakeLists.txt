# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        geo_function_test
    SOURCES
        GeoFunctionTest.cpp
    LIBRARIES
        function_manager_obj
        wkt_wkb_io_obj
        base_obj
        datatypes_obj
        time_utils_obj
        datetime_parser_obj
        time_obj
    LIBRARIES
        gtest
        gtest_main
)

nebula_add_test(
    NAME
        geo_index_test
    SOURCES
        GeoIndexTest.cpp
    LIBRARIES
        wkt_wkb_io_obj
        base_obj
        datatypes_obj
        geo_index_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gtest_main
)
