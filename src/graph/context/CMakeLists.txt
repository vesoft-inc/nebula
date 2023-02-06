# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    graph_context_obj OBJECT
    QueryContext.cpp
    QueryExpressionContext.cpp
    ExecutionContext.cpp
    Result.cpp
    Symbols.cpp
    iterator/GetNeighborsIter.cpp
    iterator/Iterator.cpp
    iterator/PropIter.cpp
    iterator/SequentialIter.cpp
    iterator/GetNbrsRespDataSetIter.cpp
)


nebula_add_subdirectory(test)
