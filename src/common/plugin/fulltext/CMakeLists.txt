# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    ft_es_graph_adapter_obj SHARED
    elasticsearch/ESGraphAdapter.cpp
)

nebula_add_library(
    ft_es_storage_adapter_obj SHARED
    elasticsearch/ESStorageAdapter.cpp
)

target_link_libraries(ft_es_storage_adapter_obj PUBLIC  process_obj    )
target_link_libraries(ft_es_graph_adapter_obj PUBLIC  process_obj    )
nebula_add_subdirectory(test)
