# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME wkt_test
    SOURCES WKTTest.cpp
)
target_link_libraries(
        wkt_test
        PRIVATE
      datatypes_obj
      base_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
    gtest gtest_main
${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
