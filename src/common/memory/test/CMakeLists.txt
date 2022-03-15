# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_executable(
  NAME sys_info_read_bm
  SOURCES SysInfoReadBenchmark.cpp
  OBJECTS
  LIBRARIES follybenchmark boost_regex ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)

nebula_add_test(
  NAME memory_utils_test
  SOURCES MemoryUtilsTest.cpp
  OBJECTS
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:memory_obj>
  LIBRARIES gtest gtest_main
)
