/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlanNode.h"
#include "planner/ExecutionPlan.h"

namespace nebula {
namespace graph {

PlanNode::PlanNode(ExecutionPlan* plan, Kind kind) : kind_(kind), plan_(plan) {
    DCHECK_NOTNULL(plan_);
    plan_->addPlanNode(this);
}

// static
const char* PlanNode::toString(PlanNode::Kind kind) {
    switch (kind) {
        case PlanNode::Kind::kUnknown:
            return "Unkonwn";
        case PlanNode::Kind::kStart:
            return "Start";
        case PlanNode::Kind::kGetNeighbors:
            return "GetNeighbors";
        case PlanNode::Kind::kGetVertices:
            return "GetVertices";
        case PlanNode::Kind::kGetEdges:
            return "GetEdges";
        case PlanNode::Kind::kReadIndex:
            return "ReadIndex";
        case PlanNode::Kind::kFilter:
            return "Filter";
        case PlanNode::Kind::kUnion:
            return "Union";
        case PlanNode::Kind::kIntersect:
            return "Intersect";
        case PlanNode::Kind::kMinus:
            return "Minus";
        case PlanNode::Kind::kProject:
            return "Project";
        case PlanNode::Kind::kSort:
            return "Sort";
        case PlanNode::Kind::kLimit:
            return "Limit";
        case PlanNode::Kind::kAggregate:
            return "Aggregate";
        case PlanNode::Kind::kSelect:
            return "Select";
        case PlanNode::Kind::kLoop:
            return "Loop";
        case PlanNode::Kind::kSwitchSpace:
            return "SwitchSpace";
        case PlanNode::Kind::kDedup:
            return "Dedup";
        case PlanNode::Kind::kMultiOutputs:
            return "MultiOutputs";
        case PlanNode::Kind::kCreateSpace:
            return "CreateSpace";
        case PlanNode::Kind::kCreateTag:
            return "CreateTag";
        case PlanNode::Kind::kCreateEdge:
            return "CreateEdge";
        case PlanNode::Kind::kDescSpace:
            return "DescSpace";
        case PlanNode::Kind::kDescTag:
            return "DescTag";
        case PlanNode::Kind::kDescEdge:
            return "DescEdge";
        case PlanNode::Kind::kAlterTag:
            return "AlterTag";
        case PlanNode::Kind::kAlterEdge:
            return "AlterEdge";
        case PlanNode::Kind::kInsertVertices:
            return "InsertVertices";
        case PlanNode::Kind::kInsertEdges:
            return "InsertEdges";
        case PlanNode::Kind::kDataCollect:
            return "DataCollect";
        case PlanNode::Kind::kShowCreateSpace:
            return "ShowCreateSpace";
        case PlanNode::Kind::kShowCreateTag:
            return "ShowCreateTag";
        case PlanNode::Kind::kShowCreateEdge:
            return "ShowCreateEdge";
        case PlanNode::Kind::kDropSpace:
            return "DropSpace";
        case PlanNode::Kind::kDropTag:
            return "DropTag";
        case PlanNode::Kind::kDropEdge:
            return "DropEdge";
        case PlanNode::Kind::kShowSpaces:
            return "kShowSpaces";
        case PlanNode::Kind::kShowTags:
            return "kShowTags";
        case PlanNode::Kind::kShowEdges:
            return "kShowEdges";
        case PlanNode::Kind::kCreateSnapshot:
            return "CreateSnapshot";
        case PlanNode::Kind::kDropSnapshot:
            return "DropSnapshot";
        case PlanNode::Kind::kShowSnapshots:
            return "ShowSnapshots";
        default:
            LOG(FATAL) << "Unknown PlanNode: " << static_cast<int64_t>(kind);
    }
}

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind) {
    os << PlanNode::toString(kind);
    return os;
}
}   // namespace graph
}   // namespace nebula
