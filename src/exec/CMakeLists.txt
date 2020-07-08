# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    exec_obj OBJECT
    Executor.cpp
    logic/LoopExecutor.cpp
    logic/MultiOutputsExecutor.cpp
    logic/StartExecutor.cpp
    logic/SelectExecutor.cpp
    query/AggregateExecutor.cpp
    query/DedupExecutor.cpp
    query/FilterExecutor.cpp
    query/GetEdgesExecutor.cpp
    query/GetNeighborsExecutor.cpp
    query/GetVerticesExecutor.cpp
    query/IntersectExecutor.cpp
    query/LimitExecutor.cpp
    query/MinusExecutor.cpp
    query/ProjectExecutor.cpp
    query/SortExecutor.cpp
    query/ReadIndexExecutor.cpp
    query/SetExecutor.cpp
    query/UnionExecutor.cpp
    query/DataCollectExecutor.cpp
    admin/SwitchSpaceExecutor.cpp
    admin/CreateSpaceExecutor.cpp
    admin/DescSpaceExecutor.cpp
    maintain/CreateTagExecutor.cpp
    maintain/CreateEdgeExecutor.cpp
    maintain/DescTagExecutor.cpp
    maintain/DescEdgeExecutor.cpp
    maintain/AlterSchemaExecutor.cpp
    mutate/InsertVerticesExecutor.cpp
    mutate/InsertEdgesExecutor.cpp
)

nebula_add_subdirectory(query/test)
nebula_add_subdirectory(logic/test)
