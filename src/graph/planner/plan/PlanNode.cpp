/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/PlanNode.h"

#include <folly/String.h>
#include <folly/json.h>

#include <memory>
#include <vector>

#include "common/graph/Response.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/PlanNodeVisitor.h"
#include "graph/util/ToJson.h"

namespace nebula {
namespace graph {

PlanNode::PlanNode(QueryContext* qctx, Kind kind) : qctx_(qctx), kind_(kind) {
  DCHECK(qctx != nullptr);
  id_ = qctx_->genId();
  auto varName = folly::stringPrintf("__%s_%ld", toString(kind_), id_);
  auto* variable = qctx_->symTable()->newVariable(varName);
  VLOG(1) << "New variable: " << varName;
  outputVar_ = variable;
  qctx_->symTable()->writtenBy(varName, this);
}

// static
const char* PlanNode::toString(PlanNode::Kind kind) {
  switch (kind) {
    case Kind::kUnknown:
      return "Unknown";
    case Kind::kStart:
      return "Start";
    case Kind::kGetNeighbors:
      return "GetNeighbors";
    case Kind::kExpand:
      return "Expand";
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
    case Kind::kScanVertices:
      return "ScanVertices";
    case Kind::kScanEdges:
      return "ScanEdges";
    case Kind::kFulltextIndexScan:
      return "FulltextIndexScan";
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
    case Kind::kSample:
      return "Sample";
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
    case Kind::kCreateSpaceAs:
      return "CreateSpaceAs";
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
    case Kind::kDescribeUser:
      return "DescribeUser";
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
    case Kind::kClearSpace:
      return "ClearSpace";
    case Kind::kDropTag:
      return "DropTag";
    case Kind::kDropEdge:
      return "DropEdge";
    case Kind::kShowSpaces:
      return "ShowSpaces";
    case Kind::kAlterSpace:
      return "AlterSpaces";
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
    case Kind::kSubmitJob:
      return "SubmitJob";
    case Kind::kInnerJoin:
      return "InnerJoin";
    case Kind::kDeleteVertices:
      return "DeleteVertices";
    case Kind::kDeleteTags:
      return "DeleteTags";
    case Kind::kDeleteEdges:
      return "DeleteEdges";
    case Kind::kUpdateVertex:
      return "UpdateVertex";
    case Kind::kUpdateEdge:
      return "UpdateEdge";
    case Kind::kShowHosts:
      return "ShowHosts";
    case Kind::kShowMetaLeader:
      return "ShowMetaLeader";
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
    case Kind::kMultiShortestPath:
      return "MultiShortestPath";
    case Kind::kProduceAllPaths:
      return "ProduceAllPaths";
    case Kind::kCartesianProduct:
      return "CartesianProduct";
    case Kind::kSubgraph:
      return "Subgraph";
    case Kind::kAddHosts:
      return "AddHosts";
    case Kind::kDropHosts:
      return "DropHosts";
    // Zone
    case Kind::kMergeZone:
      return "MergeZone";
    case Kind::kRenameZone:
      return "RenameZone";
    case Kind::kDropZone:
      return "DropZone";
    case Kind::kDivideZone:
      return "DivideZone";
    case Kind::kDescribeZone:
      return "DescribeZone";
    case Kind::kAddHostsIntoZone:
      return "AddHostsIntoZone";
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
    // service search
    case Kind::kShowServiceClients:
      return "ShowServiceClients";
    case Kind::kShowFTIndexes:
      return "ShowFTIndexes";
    case Kind::kSignInService:
      return "SignInService";
    case Kind::kSignOutService:
      return "SignOutService";
    case Kind::kShowSessions:
      return "ShowSessions";
    case Kind::kKillSession:
      return "KillSession";
    case Kind::kUpdateSession:
      return "UpdateSession";
    case Kind::kShowQueries:
      return "ShowQueries";
    case Kind::kKillQuery:
      return "KillQuery";
    case Kind::kTraverse:
      return "Traverse";
    case Kind::kAppendVertices:
      return "AppendVertices";
    case Kind::kHashLeftJoin:
      return "HashLeftJoin";
    case Kind::kHashInnerJoin:
      return "HashInnerJoin";
    case Kind::kCrossJoin:
      return "CrossJoin";
    case Kind::kShortestPath:
      return "ShortestPath";
    case Kind::kArgument:
      return "Argument";
    case Kind::kRollUpApply:
      return "RollUpApply";
    case Kind::kPatternApply:
      return "PatternApply";
    case Kind::kGetDstBySrc:
      return "GetDstBySrc";
      // no default so the compiler will warning when lack
  }
  DLOG(FATAL) << "Impossible kind plan node " << static_cast<int>(kind);
  return "Unknown";
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
  auto* outputVarPtr = qctx_->symTable()->getVar(var);
  DCHECK(outputVarPtr != nullptr);
  auto oldVar = outputVar_->name;
  outputVar_ = outputVarPtr;
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

bool PlanNode::isColumnsIncludedIn(const PlanNode* other) const {
  const auto& otherColNames = other->colNames();
  for (auto& colName : colNames()) {
    auto iter = std::find(otherColNames.begin(), otherColNames.end(), colName);
    if (iter == otherColNames.end()) {
      return false;
    }
  }
  return true;
}

std::unique_ptr<PlanNodeDescription> PlanNode::explain() const {
  auto desc = std::make_unique<PlanNodeDescription>();
  desc->id = id_;
  desc->name = toString(kind_);
  desc->outputVar = folly::toJson(util::toJson(outputVar_));
  return desc;
}

void PlanNode::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

void PlanNode::releaseSymbols() {
  auto symTbl = qctx_->symTable();
  for (auto in : inputVars_) {
    in && symTbl->deleteReadBy(in->name, this);
  }
  outputVar_ && symTbl->deleteWrittenBy(outputVar_->name, this);
}

void PlanNode::updateSymbols() {
  auto symTbl = qctx_->symTable();
  if (outputVar_ != nullptr) {
    symTbl->updateWrittenBy(outputVar_->name, outputVar_->name, this);
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

void SingleInputNode::copyInputColNames(const PlanNode* input) {
  if (input != nullptr) {
    setColNames(input->colNames());
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
  // Allow fill it later
  if (left != nullptr) {
    readVariable(left->outputVarPtr());
  } else {
    inputVars_.emplace_back(nullptr);
  }

  addDep(right);
  if (right != nullptr) {
    readVariable(right->outputVarPtr());
  } else {
    inputVars_.emplace_back(nullptr);
  }
}

// It's used for clone
BinaryInputNode::BinaryInputNode(QueryContext* qctx, Kind kind) : PlanNode(qctx, kind) {
  addDep(nullptr);
  inputVars_.emplace_back(nullptr);

  addDep(nullptr);
  inputVars_.emplace_back(nullptr);
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

void PlanNode::setColNames(std::vector<std::string> cols) {
  outputVarPtr()->colNames = std::move(cols);
}
}  // namespace graph
}  // namespace nebula
