# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,

nebula_add_executable(
    NAME snowflake_bm
    SOURCES SnowflakeBenchmark.cpp
    OBJECTS 
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:snowflake_obj>
      $<TARGET_OBJECTS:meta_client_obj>
      $<TARGET_OBJECTS:http_client_obj>
      $<TARGET_OBJECTS:thrift_obj>
      $<TARGET_OBJECTS:meta_obj>
      $<TARGET_OBJECTS:ssl_obj>
      $<TARGET_OBJECTS:conf_obj>
      $<TARGET_OBJECTS:ws_obj>
      $<TARGET_OBJECTS:ws_common_obj>
      $<TARGET_OBJECTS:process_obj>
      $<TARGET_OBJECTS:thread_obj>
      $<TARGET_OBJECTS:version_obj>
      $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
      $<TARGET_OBJECTS:meta_thrift_obj>
      $<TARGET_OBJECTS:common_thrift_obj>
      $<TARGET_OBJECTS:stats_obj>
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:wkt_wkb_io_obj>
      $<TARGET_OBJECTS:function_manager_obj>
      $<TARGET_OBJECTS:agg_function_manager_obj>
      $<TARGET_OBJECTS:time_utils_obj>
      $<TARGET_OBJECTS:datetime_parser_obj>
      $<TARGET_OBJECTS:time_obj>
      $<TARGET_OBJECTS:fs_obj>
      $<TARGET_OBJECTS:meta_client_stats_obj>
      $<TARGET_OBJECTS:expression_obj>
      $<TARGET_OBJECTS:ast_match_path_obj>
    LIBRARIES
      follybenchmark 
      boost_regex
      ${PROXYGEN_LIBRARIES}
      ${THRIFT_LIBRARIES}
)


nebula_add_test(
  NAME snowflake_test
  SOURCES SnowflakeTest.cpp
  OBJECTS
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:snowflake_obj>
    $<TARGET_OBJECTS:meta_client_obj>
    $<TARGET_OBJECTS:http_client_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:ssl_obj>
    $<TARGET_OBJECTS:conf_obj>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:function_manager_obj>
    $<TARGET_OBJECTS:agg_function_manager_obj>
    $<TARGET_OBJECTS:time_utils_obj>
    $<TARGET_OBJECTS:datetime_parser_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:meta_client_stats_obj>
    $<TARGET_OBJECTS:expression_obj>
    $<TARGET_OBJECTS:ast_match_path_obj>
  LIBRARIES 
    gtest 
    gtest_main
    ${PROXYGEN_LIBRARIES}
    ${THRIFT_LIBRARIES}
)

nebula_add_test(
  NAME segment_id_test
  SOURCES SegmentIdTest.cpp
  OBJECTS
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:segment_id_obj>
    $<TARGET_OBJECTS:thrift_obj>
  LIBRARIES 
    gtest 
    gtest_main
    ${THRIFT_LIBRARIES}
)

nebula_add_test(
  NAME segment_id_bm
  SOURCES SegmentIdBenchmark.cpp
  OBJECTS

    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:segment_id_obj>
    $<TARGET_OBJECTS:thrift_obj>
  LIBRARIES 
    follybenchmark 
    boost_regex
    ${THRIFT_LIBRARIES}
)
