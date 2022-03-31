# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,

nebula_add_executable(
    NAME snowflake_bm
    SOURCES SnowflakeBenchmark.cpp
)

target_link_libraries(
    snowflake_bm

        PRIVATE
      base_obj
      snowflake_obj
      meta_client_obj
      http_client_obj
      thrift_obj
      meta_obj
      ssl_obj
      conf_obj
      ws_obj
      ws_common_obj
      process_obj
      thread_obj
      version_obj
      file_based_cluster_id_man_obj
      meta_thrift_obj
      common_thrift_obj
      stats_obj
      datatypes_obj
      wkt_wkb_io_obj
      function_manager_obj
      agg_function_manager_obj
      time_utils_obj
      datetime_parser_obj
      time_obj
      fs_obj
      meta_client_stats_obj
      expression_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
      follybenchmark
      boost_regex
      ${PROXYGEN_LIBRARIES}
      ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
  NAME snowflake_test
  SOURCES SnowflakeTest.cpp
)
target_link_libraries(
        snowflake_test
        PRIVATE
    base_obj
    snowflake_obj
    meta_client_obj
    http_client_obj
    thrift_obj
    meta_obj
    ssl_obj
    conf_obj
    ws_obj
    ws_common_obj
    process_obj
    thread_obj
    version_obj
    file_based_cluster_id_man_obj
    meta_thrift_obj
    common_thrift_obj
    stats_obj
    datatypes_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
    wkt_wkb_io_obj
    function_manager_obj
    agg_function_manager_obj
    time_utils_obj
    datetime_parser_obj
    time_obj
    fs_obj
    meta_client_stats_obj
    expression_obj
    gtest
    gtest_main
    ${PROXYGEN_LIBRARIES}
    ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
