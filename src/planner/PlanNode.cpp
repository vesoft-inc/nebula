/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlanNode.h"

#include "common/interface/gen-cpp2/graph_types.h"
#include "context/QueryContext.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

PlanNode::PlanNode(int64_t id, Kind kind) : kind_(kind), id_(id) {
    DCHECK_GE(id_, 0);
    outputVar_ = folly::stringPrintf("__%s_%ld", toString(kind_), id_);
}

// static
const char* PlanNode::toString(PlanNode::Kind kind) {
    switch (kind) {
        case Kind::kUnknown:
            return "Unkonwn";
        case Kind::kStart:
            return "Start";
        case Kind::kGetNeighbors:
            return "GetNeighbors";
        case Kind::kGetVertices:
            return "GetVertices";
        case Kind::kGetEdges:
            return "GetEdges";
        case Kind::kIndexScan:
            return "IndexScan";
        case Kind::kFilter:
            return "Filter";
        case Kind::kUnion:
            return "Union";
        case Kind::kIntersect:
            return "Intersect";
        case Kind::kMinus:
            return "Minus";
        case Kind::kProject:
            return "Project";
        case Kind::kSort:
            return "Sort";
        case Kind::kLimit:
            return "Limit";
        case Kind::kAggregate:
            return "Aggregate";
        case Kind::kSelect:
            return "Select";
        case Kind::kLoop:
            return "Loop";
        case Kind::kDedup:
            return "Dedup";
        case Kind::kPassThrough:
            return "PassThrough";
        case Kind::kSwitchSpace:
            return "RegisterSpaceToSession";
        case Kind::kCreateSpace:
            return "CreateSpace";
        case Kind::kCreateTag:
            return "CreateTag";
        case Kind::kCreateEdge:
            return "CreateEdge";
        case Kind::kDescSpace:
            return "DescSpace";
        case Kind::kDescTag:
            return "DescTag";
        case Kind::kDescEdge:
            return "DescEdge";
        case Kind::kAlterTag:
            return "AlterTag";
        case Kind::kAlterEdge:
            return "AlterEdge";
        case Kind::kInsertVertices:
            return "InsertVertices";
        case Kind::kInsertEdges:
            return "InsertEdges";
        case Kind::kDataCollect:
            return "DataCollect";
        // ACL
        case Kind::kCreateUser:
            return "CreateUser";
        case Kind::kDropUser:
            return "DropUser";
        case Kind::kUpdateUser:
            return "UpdateUser";
        case Kind::kGrantRole:
            return "GrantRole";
        case Kind::kRevokeRole:
            return "RevokeRole";
        case Kind::kChangePassword:
            return "ChangePassword";
        case Kind::kListUserRoles:
            return "ListUserRoles";
        case Kind::kListUsers:
            return "ListUsers";
        case Kind::kListRoles:
            return "ListRoles";
        case Kind::kShowCreateSpace:
            return "ShowCreateSpace";
        case Kind::kShowCreateTag:
            return "ShowCreateTag";
        case Kind::kShowCreateEdge:
            return "ShowCreateEdge";
        case Kind::kDropSpace:
            return "DropSpace";
        case Kind::kDropTag:
            return "DropTag";
        case Kind::kDropEdge:
            return "DropEdge";
        case Kind::kShowSpaces:
            return "ShowSpaces";
        case Kind::kShowTags:
            return "ShowTags";
        case Kind::kShowEdges:
            return "ShowEdges";
        case Kind::kCreateSnapshot:
            return "CreateSnapshot";
        case Kind::kDropSnapshot:
            return "DropSnapshot";
        case Kind::kShowSnapshots:
            return "ShowSnapshots";
        case Kind::kBalanceLeaders:
            return "BalanceLeaders";
        case Kind::kBalance:
            return "Balance";
        case Kind::kStopBalance:
            return "StopBalance";
        case Kind::kShowBalance:
            return "ShowBalance";
        case Kind::kSubmitJob:
            return "SubmitJob";
        case Kind::kDataJoin:
            return "DataJoin";
        case Kind::kDeleteVertices:
            return "DeleteVertices";
        case Kind::kDeleteEdges:
            return "DeleteEdges";
        case Kind::kUpdateVertex:
            return "UpdateVertex";
        case Kind::kUpdateEdge:
            return "UpdateEdge";
        case Kind::kShowHosts:
            return "ShowHosts";
        case Kind::kShowParts:
            return "ShowParts";
        case Kind::kShowCharset:
            return "ShowCharset";
        case Kind::kShowCollation:
            return "ShowCollation";
        case Kind::kShowConfigs:
            return "ShowConfigs";
        case Kind::kSetConfig:
            return "SetConfig";
        case Kind::kGetConfig:
            return "GetConfig";
            // no default so the compiler will warning when lack
    }
    LOG(FATAL) << "Impossible kind plan node " << static_cast<int>(kind);
}

// static
void PlanNode::addDescription(std::string key, std::string value, cpp2::PlanNodeDescription* desc) {
    if (!desc->__isset.description) {
        desc->set_description({});
    }
    cpp2::Pair kv;
    kv.set_key(std::move(key));
    kv.set_value(std::move(value));
    desc->get_description()->emplace_back(std::move(kv));
}

void PlanNode::calcCost() {
    VLOG(1) << "unimplemented cost calculation.";
}

std::unique_ptr<cpp2::PlanNodeDescription> PlanNode::explain() const {
    auto desc = std::make_unique<cpp2::PlanNodeDescription>();
    desc->set_id(id_);
    desc->set_name(toString(kind_));
    desc->set_output_var(outputVar_);
    addDescription("colNames", folly::toJson(util::toJson(colNames_)), desc.get());
    return desc;
}

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind) {
    os << PlanNode::toString(kind);
    return os;
}

std::unique_ptr<cpp2::PlanNodeDescription> SingleDependencyNode::explain() const {
    auto desc = PlanNode::explain();
    DCHECK(!desc->__isset.dependencies);
    desc->set_dependencies({dep()->id()});
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> SingleInputNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("inputVar", inputVar_, desc.get());
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> BiInputNode::explain() const {
    auto desc = PlanNode::explain();
    DCHECK(!desc->__isset.dependencies);
    desc->set_dependencies({left()->id(), right()->id()});
    addDescription("leftVar", leftVar_, desc.get());
    addDescription("rightVar", rightVar_, desc.get());
    return desc;
}

}   // namespace graph
}   // namespace nebula
