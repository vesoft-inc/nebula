# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME expression_encode_decode_test
    SOURCES EncodeDecodeTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)
