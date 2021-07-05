/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/PlanNode.h"

#include <memory>
#include <vector>

#include <folly/String.h>
#include <folly/json.h>

#include "common/graph/Response.h"
#include "context/QueryContext.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

PlanNode::PlanNode(QueryContext* qctx, Kind kind) : qctx_(qctx), kind_(kind) {
    DCHECK(qctx != nullptr);
    id_ = qctx_->genId();
    auto varName = folly::stringPrintf("__%s_%ld", toString(kind_), id_);
    auto* variable = qctx_->symTable()->newVariable(varName);
    VLOG(1) << "New variable: " << varName;
    outputVars_.emplace_back(variable);
    qctx_->symTable()->writtenBy(varName, this);
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
        case Kind::kTagIndexFullScan:
            return "TagIndexFullScan";
        case Kind::kTagIndexRangeScan:
            return "TagIndexRangeScan";
        case Kind::kTagIndexPrefixScan:
            return "TagIndexPrefixScan";
        case Kind::kEdgeIndexFullScan:
            return "EdgeIndexFullScan";
        case Kind::kEdgeIndexRangeScan:
            return "EdgeIndexRangeScan";
        case Kind::kEdgeIndexPrefixScan:
            return "EdgeIndexPrefixScan";
        case Kind::kFilter:
            return "Filter";
        case Kind::kUnion:
            return "Union";
        case Kind::kUnionAllVersionVar:
            return "UnionAllVersionVar";
        case Kind::kIntersect:
            return "Intersect";
        case Kind::kMinus:
            return "Minus";
        case Kind::kProject:
            return "Project";
        case Kind::kUnwind:
            return "Unwind";
        case Kind::kSort:
            return "Sort";
        case Kind::kTopN:
            return "TopN";
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
        case Kind::kAssign:
            return "Assign";
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
        case Kind::kCreateTagIndex:
            return "CreateTagIndex";
        case Kind::kCreateEdgeIndex:
            return "CreateEdgeIndex";
        case Kind::kCreateFTIndex:
            return "CreateFTIndex";
        case Kind::kDropTagIndex:
            return "DropTagIndex";
        case Kind::kDropEdgeIndex:
            return "DropEdgeIndex";
        case Kind::kDropFTIndex:
            return "DropFTIndex";
        case Kind::kDescTagIndex:
            return "DescTagIndex";
        case Kind::kDescEdgeIndex:
            return "DescEdgeIndex";
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
        case Kind::kShowCreateTagIndex:
            return "ShowCreateTagIndex";
        case Kind::kShowCreateEdgeIndex:
            return "ShowCreateEdgeIndex";
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
        case Kind::kShowTagIndexes:
            return "ShowTagIndexes";
        case Kind::kShowEdgeIndexes:
            return "ShowEdgeIndexes";
        case Kind::kShowTagIndexStatus:
            return "ShowTagIndexStatus";
        case Kind::kShowEdgeIndexStatus:
            return "ShowEdgeIndexStatus";
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
        case Kind::kResetBalance:
            return "ResetBalance";
        case Kind::kShowBalance:
            return "ShowBalance";
        case Kind::kSubmitJob:
            return "SubmitJob";
        case Kind::kLeftJoin:
            return "LeftJoin";
        case Kind::kInnerJoin:
            return "InnerJoin";
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
        case Kind::kBFSShortest:
            return "BFSShortest";
        case Kind::kProduceSemiShortestPath:
            return "ProduceSemiShortestPath";
        case Kind::kConjunctPath:
            return "ConjunctPath";
        case Kind::kProduceAllPaths:
            return "ProduceAllPaths";
        case Kind::kCartesianProduct:
            return "CartesianProduct";
        case Kind::kSubgraph:
            return "Subgraph";
        // Group and Zone
        case Kind::kAddGroup:
            return "AddGroup";
        case Kind::kDropGroup:
            return "DropGroup";
        case Kind::kAddZone:
            return "AddZone";
        case Kind::kDropZone:
            return "DropZone";
        case Kind::kDescribeGroup:
            return "DescribeGroup";
        case Kind::kAddZoneIntoGroup:
            return "AddZoneIntoGroup";
        case Kind::kDropZoneFromGroup:
            return "DropZoneFromGroup";
        case Kind::kDescribeZone:
            return "DescribeZone";
        case Kind::kAddHostIntoZone:
            return "AddHostIntoZone";
        case Kind::kDropHostFromZone:
            return "DropHostFromZone";
        case Kind::kShowGroups:
            return "ShowGroups";
        case Kind::kShowZones:
            return "ShowZones";
        case Kind::kAddListener:
            return "AddListener";
        case Kind::kRemoveListener:
            return "RemoveListener";
        case Kind::kShowListener:
            return "ShowListener";
        case Kind::kShowStats:
            return "ShowStats";
        // text search
        case Kind::kShowTSClients:
            return "ShowTSClients";
        case Kind::kShowFTIndexes:
            return "ShowFTIndexes";
        case Kind::kSignInTSService:
            return "SignInTSService";
        case Kind::kSignOutTSService:
            return "SignOutTSService";
        case Kind::kDownload:
            return "Download";
        case Kind::kIngest:
            return "Ingest";
        // no default so the compiler will warning when lack
        case Kind::kShowSessions:
            return "ShowSessions";
        case Kind::kUpdateSession:
            return "UpdateSession";
        case Kind::kShowQueries:
            return "ShowQueries";
        case Kind::kKillQuery:
            return "KillQuery";
            // no default so the compiler will warning when lack
    }
    LOG(FATAL) << "Impossible kind plan node " << static_cast<int>(kind);
}

std::string PlanNode::toString() const {
    return folly::stringPrintf("%s_%ld", toString(kind_), id_);
}

// static
void PlanNode::addDescription(std::string key, std::string value, PlanNodeDescription* desc) {
    if (desc->description == nullptr) {
        desc->description = std::make_unique<std::vector<Pair>>();
    }
    desc->description->emplace_back(Pair{std::move(key), std::move(value)});
}

void PlanNode::readVariable(const std::string& varname) {
    auto varPtr = qctx_->symTable()->getVar(varname);
    readVariable(varPtr);
}

void PlanNode::readVariable(Variable* varPtr) {
    DCHECK(varPtr != nullptr);
    inputVars_.emplace_back(varPtr);
    qctx_->symTable()->readBy(varPtr->name, this);
}

void PlanNode::calcCost() {
    VLOG(1) << "unimplemented cost calculation.";
}

void PlanNode::setOutputVar(const std::string& var) {
    DCHECK_EQ(1, outputVars_.size());
    auto* outputVarPtr = qctx_->symTable()->getVar(var);
    DCHECK(outputVarPtr != nullptr);
    auto oldVar = outputVars_[0]->name;
    outputVars_[0] = outputVarPtr;
    qctx_->symTable()->updateWrittenBy(oldVar, var, this);
}

void PlanNode::setInputVar(const std::string& varname, size_t idx) {
    std::string oldVar = inputVar(idx);
    auto symTable = qctx_->symTable();
    auto varPtr = symTable->getVar(varname);
    DCHECK(varPtr != nullptr);
    inputVars_[idx] = varPtr;
    if (!oldVar.empty()) {
        symTable->updateReadBy(oldVar, varname, this);
    } else {
        symTable->readBy(varname, this);
    }
}

std::unique_ptr<PlanNodeDescription> PlanNode::explain() const {
    auto desc = std::make_unique<PlanNodeDescription>();
    desc->id = id_;
    desc->name = toString(kind_);
    desc->outputVar = folly::toJson(util::toJson(outputVars_));
    return desc;
}

void PlanNode::releaseSymbols() {
    auto symTbl = qctx_->symTable();
    for (auto in : inputVars_) {
        in && symTbl->deleteReadBy(in->name, this);
    }
    for (auto out : outputVars_) {
        out && symTbl->deleteWrittenBy(out->name, this);
    }
}

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind) {
    os << PlanNode::toString(kind);
    return os;
}

SingleInputNode::SingleInputNode(QueryContext* qctx, Kind kind, const PlanNode* dep)
    : SingleDependencyNode(qctx, kind, dep) {
    if (dep != nullptr) {
        readVariable(dep->outputVarPtr());
    } else {
        inputVars_.emplace_back(nullptr);
    }
}

std::unique_ptr<PlanNodeDescription> SingleDependencyNode::explain() const {
    auto desc = PlanNode::explain();
    DCHECK(desc->dependencies == nullptr);
    desc->dependencies.reset(new std::vector<int64_t>{dep()->id()});
    return desc;
}

std::unique_ptr<PlanNodeDescription> SingleInputNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("inputVar", inputVar(), desc.get());
    return desc;
}

BinaryInputNode::BinaryInputNode(QueryContext* qctx,
                                 Kind kind,
                                 const PlanNode* left,
                                 const PlanNode* right)
    : PlanNode(qctx, kind) {
    addDep(left);
    readVariable(left->outputVarPtr());

    addDep(right);
    readVariable(right->outputVarPtr());
}

std::unique_ptr<PlanNodeDescription> BinaryInputNode::explain() const {
    auto desc = PlanNode::explain();
    DCHECK(desc->dependencies == nullptr);
    desc->dependencies.reset(new std::vector<int64_t>{left()->id(), right()->id()});
    folly::dynamic inputVar = folly::dynamic::object();
    inputVar.insert("leftVar", leftInputVar());
    inputVar.insert("rightVar", rightInputVar());
    addDescription("inputVar", folly::toJson(inputVar), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> VariableDependencyNode::explain() const {
    auto desc = PlanNode::explain();
    DCHECK(desc->dependencies == nullptr);
    desc->dependencies.reset(new std::vector<int64_t>(dependIds()));
    return desc;
}

}   // namespace graph
}   // namespace nebula
