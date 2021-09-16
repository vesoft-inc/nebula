# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_executable(
  NAME sys_info_read_bm
  SOURCES SysInfoReadBenchmark.cpp
  OBJECTS
  LIBRARIES follybenchmark boost_regex ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)
