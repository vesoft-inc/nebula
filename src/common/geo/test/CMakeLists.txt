# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        geo_function_test
    SOURCES
        GeoFunctionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:datetime_parser_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        gtest
        gtest_main
)

nebula_add_test(
    NAME
        geo_index_test
    SOURCES
        GeoIndexTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:geo_index_obj>
        $<TARGET_OBJECTS:storage_thrift_obj>
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gtest_main
)
