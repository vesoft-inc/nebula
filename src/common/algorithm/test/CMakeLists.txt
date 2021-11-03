# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME reservoir_sampling_test
    SOURCES ReservoirSamplingTest.cpp
    OBJECTS $<TARGET_OBJECTS:time_obj>
    LIBRARIES gtest gtest_main
)
