# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_executable(
  NAME sys_info_read_bm
  SOURCES SysInfoReadBenchmark.cpp
)
target_link_libraries(
        sys_info_read_bm
        PRIVATE
  follybenchmark
  boost_regex
  ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
  NAME memory_utils_test
  SOURCES MemoryUtilsTest.cpp
)
target_link_libraries(
        memory_utils_test
        PRIVATE
    base_obj
    fs_obj
    memory_obj
  gtest gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
