# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME reservoir_sampling_test
    SOURCES ReservoirSamplingTest.cpp
    OBJECTS $<TARGET_OBJECTS:time_obj>
    LIBRARIES gtest gtest_main
)
