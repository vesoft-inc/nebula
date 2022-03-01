# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    planner_obj SHARED
    Planner.cpp
    PlannersRegister.cpp
    SequentialPlanner.cpp
    match/MatchSolver.cpp
    match/SegmentsConnector.cpp
    match/MatchPlanner.cpp
    match/MatchClausePlanner.cpp
    match/UnwindClausePlanner.cpp
    match/ReturnClausePlanner.cpp
    match/OrderByClausePlanner.cpp
    match/YieldClausePlanner.cpp
    match/PaginationPlanner.cpp
    match/WhereClausePlanner.cpp
    match/WithClausePlanner.cpp
    match/StartVidFinder.cpp
    match/PropIndexSeek.cpp
    match/VertexIdSeek.cpp
    match/LabelIndexSeek.cpp
    match/ScanSeek.cpp
    match/ArgumentFinder.cpp
    ngql/PathPlanner.cpp
    ngql/GoPlanner.cpp
    ngql/SubgraphPlanner.cpp
    ngql/LookupPlanner.cpp
    ngql/FetchVerticesPlanner.cpp
    ngql/FetchEdgesPlanner.cpp
    ngql/MaintainPlanner.cpp
)

nebula_add_library(
    plan_obj SHARED
    plan/PlanNode.cpp
    plan/ExecutionPlan.cpp
    plan/Algo.cpp
    plan/Admin.cpp
    plan/Logic.cpp
    plan/Query.cpp
    plan/Mutate.cpp
    plan/Maintain.cpp
)
target_link_libraries(plan_obj PUBLIC ${Glog_LIBRARY} idgenerator_obj graph_context_obj )
