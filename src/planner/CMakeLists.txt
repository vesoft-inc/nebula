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
    planners/SequentialPlanner.cpp
    planners/MatchSolver.cpp
    planners/MatchVertexIdSeekPlanner.cpp
    planners/MatchVertexIndexSeekPlanner.cpp
    planners/MatchTagScanPlanner.cpp
)
