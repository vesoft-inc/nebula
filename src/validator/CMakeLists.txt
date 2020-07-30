# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    validator_obj OBJECT
    Validator.cpp
    AssignmentValidator.cpp
    ASTValidator.cpp
    GoValidator.cpp
    PipeValidator.cpp
    SequentialValidator.cpp
    SetValidator.cpp
    UseValidator.cpp
    GetSubgraphValidator.cpp
    AdminValidator.cpp
    MaintainValidator.cpp
    MutateValidator.cpp
    LimitValidator.cpp
    OrderByValidator.cpp
    YieldValidator.cpp
    ExplainValidator.cpp
)

add_subdirectory(test)
