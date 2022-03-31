# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME reservoir_sampling_test
    SOURCES ReservoirSamplingTest.cpp
)
target_link_libraries(
     reservoir_sampling_test
        PRIVATE
        time_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
