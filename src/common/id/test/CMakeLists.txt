# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,

nebula_add_executable(
    NAME snowflake_bm
    SOURCES SnowflakeBenchmark.cpp
    OBJECTS 
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:snowflake_obj>
    LIBRARIES follybenchmark boost_regex
)


nebula_add_test(
  NAME snowflake_test
  SOURCES SnowflakeTest.cpp
  OBJECTS
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:snowflake_obj>
  LIBRARIES gtest gtest_main
)