# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    optimizer_obj
    OBJECT
    OptimizerUtils.cpp
    Optimizer.cpp
    OptGroup.cpp
    OptRule.cpp
    rule/PushFilterDownGetNbrsRule.cpp
    rule/IndexScanRule.cpp
    rule/TopNRule.cpp
)

nebula_add_subdirectory(test)
