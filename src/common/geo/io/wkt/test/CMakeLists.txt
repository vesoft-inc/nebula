# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME wkt_test
    SOURCES WKTTest.cpp
    OBJECTS
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:wkt_wkb_io_obj>
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)
