# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    planner_obj OBJECT
    PlanNode.cpp
    ExecutionPlan.cpp
    Algo.cpp
    Admin.cpp
    Logic.cpp
    Query.cpp
    Mutate.cpp
    Maintain.cpp
    Planner.cpp
    PlannersRegister.cpp
    SequentialPlanner.cpp
    match/MatchSolver.cpp
    match/SegmentsConnector.cpp
    match/InnerJoinStrategy.cpp
    match/AddDependencyStrategy.cpp
    match/AddInputStrategy.cpp
    match/CartesianProductStrategy.cpp
    match/MatchPlanner.cpp
    match/MatchClausePlanner.cpp
    match/UnwindClausePlanner.cpp
    match/ReturnClausePlanner.cpp
    match/OrderByClausePlanner.cpp
    match/PaginationPlanner.cpp
    match/WhereClausePlanner.cpp
    match/WithClausePlanner.cpp
    match/StartVidFinder.cpp
    match/PropIndexSeek.cpp
    match/VertexIdSeek.cpp
    match/Expand.cpp
)
