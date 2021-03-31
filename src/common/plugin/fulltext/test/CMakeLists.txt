# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        plugin_fulltext_test
    SOURCES
        FulltextPluginTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:encryption_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:ft_es_storage_adapter_obj>
        $<TARGET_OBJECTS:ft_es_graph_adapter_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        gtest
)
