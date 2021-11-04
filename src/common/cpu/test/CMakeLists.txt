# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        cpu_binder_test
    SOURCES
        CpuBinderTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:cpu_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)
