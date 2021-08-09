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
    OptContext.cpp
    rule/PushFilterDownGetNbrsRule.cpp
    rule/RemoveNoopProjectRule.cpp
    rule/CombineFilterRule.cpp
    rule/CollapseProjectRule.cpp
    rule/MergeGetVerticesAndDedupRule.cpp
    rule/MergeGetVerticesAndProjectRule.cpp
    rule/MergeGetNbrsAndDedupRule.cpp
    rule/MergeGetNbrsAndProjectRule.cpp
    rule/IndexScanRule.cpp
    rule/LimitPushDownRule.cpp
    rule/TopNRule.cpp
    rule/PushFilterDownAggregateRule.cpp
    rule/PushFilterDownProjectRule.cpp
    rule/PushFilterDownLeftJoinRule.cpp
    rule/OptimizeEdgeIndexScanByFilterRule.cpp
    rule/OptimizeTagIndexScanByFilterRule.cpp
    rule/UnionAllIndexScanBaseRule.cpp
    rule/UnionAllTagIndexScanRule.cpp
    rule/UnionAllEdgeIndexScanRule.cpp
    rule/IndexFullScanBaseRule.cpp
    rule/TagIndexFullScanRule.cpp
    rule/EdgeIndexFullScanRule.cpp
)

nebula_add_subdirectory(test)
