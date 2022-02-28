# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    graph_context_obj SHARED
    QueryContext.cpp
    QueryExpressionContext.cpp
    ExecutionContext.cpp
    Iterator.cpp
    Result.cpp
    Symbols.cpp
)

target_link_libraries(graph_context_obj PUBLIC ${Glog_LIBRARY} graph_flags_obj memory_obj)

nebula_add_subdirectory(test)
