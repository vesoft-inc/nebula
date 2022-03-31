# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME configuration_test
    SOURCES ConfigurationTest.cpp
)
target_link_libraries(
        configuration_test
        PRIVATE
    conf_obj fs_obj base_obj
    gtest gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
