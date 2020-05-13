# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME query_context_test
    SOURCES QueryContextTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:context_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
    LIBRARIES gtest
)

