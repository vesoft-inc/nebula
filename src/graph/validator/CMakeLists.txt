# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    validator_obj OBJECT
    Validator.cpp
    AssignmentValidator.cpp
    GoValidator.cpp
    PipeValidator.cpp
    SequentialValidator.cpp
    SetValidator.cpp
    UseValidator.cpp
    GetSubgraphValidator.cpp
    AdminValidator.cpp
    BalanceValidator.cpp
    AdminJobValidator.cpp
    MaintainValidator.cpp
    MutateValidator.cpp
    ACLValidator.cpp
    FetchEdgesValidator.cpp
    FetchVerticesValidator.cpp
    LimitValidator.cpp
    OrderByValidator.cpp
    YieldValidator.cpp
    TraversalValidator.cpp
    ExplainValidator.cpp
    GroupByValidator.cpp
    FindPathValidator.cpp
    LookupValidator.cpp
    MatchValidator.cpp
    DownloadValidator.cpp
    IngestValidator.cpp
)

nebula_add_subdirectory(test)
