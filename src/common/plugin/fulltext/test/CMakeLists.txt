# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        plugin_fulltext_test
    SOURCES
        FulltextPluginTest.cpp
    LIBRARIES
        base_obj
        network_obj
        fs_obj
        process_obj
        ft_es_storage_adapter_obj
        ft_es_graph_adapter_obj
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        gtest
)
target_link_libraries(
        plugin_fulltext_test
        PRIVATE
        base_obj
        network_obj
        fs_obj
        process_obj
        ft_es_storage_adapter_obj
        ft_es_graph_adapter_obj
        ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        gtest
        ${NEBULA_LIBS}
)
