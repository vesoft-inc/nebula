# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(UTIL_TEST_FLAG_DEPS
    $<TARGET_OBJECTS:graph_flags_obj>
)

if(ENABLE_STANDALONE_VERSION)
set(UTIL_TEST_FLAG_DEPS
    ${UTIL_TEST_FLAG_DEPS}
    $<TARGET_OBJECTS:sa_test_graph_flags_obj>
)
endif()
find_library(Boost_Thread_LIBRARY NAMES libboost_thread.a)

if(Boost_Thread_LIBRARY)
    mark_as_advanced(
        Boost_Thread_LIBRARY
    )
else()
    message(FATAL_ERROR "boost_thread doesn't exist")
endif()

nebula_add_test(
    NAME utils_test
    SOURCES
        ExpressionUtilsTest.cpp
        IdGeneratorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:ast_match_path_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:meta_client_obj>
        $<TARGET_OBJECTS:meta_obj>
        $<TARGET_OBJECTS:storage_thrift_obj>
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:conf_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
        $<TARGET_OBJECTS:charset_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:datetime_parser_obj>
        $<TARGET_OBJECTS:graph_obj>
        $<TARGET_OBJECTS:ws_common_obj>
        $<TARGET_OBJECTS:version_obj>
        $<TARGET_OBJECTS:ssl_obj>
        $<TARGET_OBJECTS:idgenerator_obj>
        $<TARGET_OBJECTS:expr_visitor_obj>
        $<TARGET_OBJECTS:graph_session_obj>
        ${UTIL_TEST_FLAG_DEPS}
        $<TARGET_OBJECTS:util_obj>
        $<TARGET_OBJECTS:plan_obj>
        $<TARGET_OBJECTS:parser_obj>
        $<TARGET_OBJECTS:ast_match_path_obj>
        $<TARGET_OBJECTS:graph_context_obj>
        $<TARGET_OBJECTS:memory_obj>
        $<TARGET_OBJECTS:version_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:graph_stats_obj>
        $<TARGET_OBJECTS:meta_client_stats_obj>
        $<TARGET_OBJECTS:storage_client_stats_obj>
        $<TARGET_OBJECTS:gc_obj>
        $<TARGET_OBJECTS:es_adapter_obj>
        $<TARGET_OBJECTS:http_client_obj>
    LIBRARIES
        gtest
        gmock
        gtest_main
        ${Boost_Thread_LIBRARY}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        curl
)
