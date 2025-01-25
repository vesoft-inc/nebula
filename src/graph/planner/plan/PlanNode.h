/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLAN_PLANNODE_H_
#define GRAPH_PLANNER_PLAN_PLANNODE_H_

#include "common/expression/Expression.h"
#include "common/graph/Response.h"
#include "graph/context/QueryContext.h"
#include "graph/context/Symbols.h"

namespace nebula {
namespace graph {

class PlanNodeVisitor;

// PlanNode is an abstraction of nodes in an execution plan which
// is a kind of directed cyclic graph.
class PlanNode {
 public:
  enum class Kind : uint8_t {
    kUnknown = 0,

    // Query
    kGetNeighbors,
    kGetVertices,
    kGetEdges,
    kExpand,
    kExpandAll,
    kTraverse,
    kAppendVertices,
    kShortestPath,

    // ------------------
    // TODO(yee): refactor in logical plan
    kIndexScan,
    kTagIndexFullScan,
    kTagIndexPrefixScan,
    kTagIndexRangeScan,
    kEdgeIndexFullScan,
    kEdgeIndexPrefixScan,
    kEdgeIndexRangeScan,
    kScanVertices,
    kScanEdges,
    kFulltextIndexScan,
    kVectorIndexScan,
    // direct value
    kValue,

    // ------------------
    kFilter,
    kUnion,
    kUnionAllVersionVar,
    kIntersect,
    kMinus,
    kProject,
    kUnwind,
    kSort,
    kTopN,
    kLimit,
    kSample,
    kAggregate,
    kDedup,
    kAssign,
    kBFSShortest,
    kMultiShortestPath,
    kAllPaths,
    kCartesianProduct,
    kSubgraph,
    kDataCollect,
    kInnerJoin,
    kHashLeftJoin,
    kHashInnerJoin,
    kCrossJoin,
    kRollUpApply,
    kPatternApply,
    kArgument,

    // Logic
    kSelect,
    kLoop,
    kPassThrough,
    kStart,

    // schema related
    kCreateSpace,
    kCreateSpaceAs,
    kCreateTag,
    kCreateEdge,
    kDescSpace,
    kShowCreateSpace,
    kDescTag,
    kDescEdge,
    kAlterTag,
    kAlterEdge,
    kShowSpaces,
    kSwitchSpace,
    kShowTags,
    kShowEdges,
    kShowCreateTag,
    kShowCreateEdge,
    kDropSpace,
    kClearSpace,
    kDropTag,
    kDropEdge,
    kAlterSpace,

    // index related
    kCreateTagIndex,
    kCreateEdgeIndex,
    kCreateFTIndex,
    kDropFTIndex,
    kDropTagIndex,
    kDropEdgeIndex,
    kDescTagIndex,
    kDescEdgeIndex,
    kShowCreateTagIndex,
    kShowCreateEdgeIndex,
    kShowTagIndexes,
    kShowEdgeIndexes,
    kShowTagIndexStatus,
    kShowEdgeIndexStatus,
    kInsertVertices,
    kInsertEdges,
    kSubmitJob,
    kShowHosts,

    // user related
    kCreateUser,
    kDropUser,
    kUpdateUser,
    kGrantRole,
    kRevokeRole,
    kChangePassword,
    kListUserRoles,
    kListUsers,
    kListRoles,
    kDescribeUser,

    // Snapshot
    kCreateSnapshot,
    kDropSnapshot,
    kShowSnapshots,

    // Update/Delete
    kDeleteVertices,
    kDeleteEdges,
    kUpdateVertex,
    kDeleteTags,
    kUpdateEdge,

    // Show
    kShowParts,
    kShowCharset,
    kShowCollation,
    kShowStats,
    kShowConfigs,
    kSetConfig,
    kGetConfig,
    kShowMetaLeader,

    // zone related
    kShowZones,
    kMergeZone,
    kRenameZone,
    kDropZone,
    kDivideZone,
    kAddHosts,
    kDropHosts,
    kDescribeZone,
    kAddHostsIntoZone,

    // listener related
    kAddListener,
    kRemoveListener,
    kShowListener,

    // service related
    kShowServiceClients,
    kShowFTIndexes,
    kSignInService,
    kSignOutService,
    kShowSessions,
    kUpdateSession,
    kKillSession,

    kShowQueries,
    kKillQuery,

    kCreateVectorIndex,
    kDropVectorIndex,
  };

  bool isQueryNode() const {
    return kind_ <= Kind::kStart;
  }

  // Describe plan node
  virtual std::unique_ptr<PlanNodeDescription> explain() const;

  virtual void accept(PlanNodeVisitor* visitor);

  void markDeleted() {
    deleted_ = true;
  }

  virtual PlanNode* clone() const = 0;

  virtual void calcCost();

  Kind kind() const {
    return kind_;
  }

  int64_t id() const {
    return id_;
  }

  QueryContext* qctx() const {
    return qctx_;
  }

  bool isSingleInput() const {
    return numDeps() == 1U;
  }

  bool isBiInput() const {
    return numDeps() == 2U;
  }

  void setOutputVar(const std::string& var);

  const std::string& outputVar() const {
    return outputVarPtr()->name;
  }

  Variable* outputVarPtr() const {
    return outputVar_;
  }

  const std::vector<std::string>& colNames() const {
    return outputVarPtr()->colNames;
  }

  void setId(int64_t id) {
    id_ = id;
  }

  void setColNames(std::vector<std::string> cols);

  const auto& dependencies() const {
    return dependencies_;
  }

  auto& dependencies() {
    return dependencies_;
  }

  const PlanNode* dep(size_t index = 0) const {
    DCHECK_LT(index, dependencies_.size());
    return dependencies_.at(index);
  }

  void setDep(size_t index, const PlanNode* dep) {
    DCHECK_LT(index, dependencies_.size());
    dependencies_[index] = DCHECK_NOTNULL(dep);
  }

  void addDep(const PlanNode* dep) {
    dependencies_.emplace_back(dep);
  }

  size_t numDeps() const {
    return dependencies_.size();
  }

  std::string inputVar(size_t idx = 0UL) const {
    DCHECK_LT(idx, inputVars_.size());
    return inputVars_[idx] ? inputVars_[idx]->name : "";
  }

  void setInputVar(const std::string& varname, size_t idx = 0UL);

  const std::vector<Variable*>& inputVars() const {
    return inputVars_;
  }

  void releaseSymbols();

  void updateSymbols();

  static const char* toString(Kind kind);
  std::string toString() const;

  double cost() const {
    return cost_;
  }

  void setLoopLayers(std::size_t c) {
    loopLayers_ = c;
  }

  std::size_t loopLayers() const {
    return loopLayers_;
  }

  template <typename T>
  const T* asNode() const {
    static_assert(std::is_base_of<PlanNode, T>::value, "T must be a subclass of PlanNode");
    DCHECK(dynamic_cast<const T*>(this) != nullptr);
    return static_cast<const T*>(this);
  }

  bool isColumnsIncludedIn(const PlanNode* other) const;

 protected:
  PlanNode(QueryContext* qctx, Kind kind);

  virtual ~PlanNode() = default;

  static void addDescription(std::string key, std::string value, PlanNodeDescription* desc);

  void readVariable(const std::string& varname);
  void readVariable(Variable* varPtr);
  void cloneMembers(const PlanNode& node) {
    // TODO maybe shall copy cost_ and dependencies_ too
    inputVars_ = node.inputVars_;
    // OutputVar will generated in constructor
    setColNames(node.colNames());
  }

  QueryContext* qctx_{nullptr};
  Kind kind_{Kind::kUnknown};
  int64_t id_{-1};
  double cost_{0.0};
  std::vector<const PlanNode*> dependencies_;
  std::vector<Variable*> inputVars_;
  Variable* outputVar_;
  // nested loop layers of current node
  std::size_t loopLayers_{0};
  bool deleted_{false};
};

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind);

// Dependencies will cover the inputs, For example bi input require bi
// dependencies as least, but single dependencies may don't need any inputs (I.E
// admin plan node) Single dependency without input It's useful for admin plan
// node
class SingleDependencyNode : public PlanNode {
 public:
  void dependsOn(const PlanNode* dep) {
    setDep(0, dep);
  }

  PlanNode* clone() const override {
    DLOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

 protected:
  SingleDependencyNode(QueryContext* qctx, Kind kind, const PlanNode* dep) : PlanNode(qctx, kind) {
    addDep(dep);
  }

  void cloneMembers(const SingleDependencyNode& node) {
    PlanNode::cloneMembers(node);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;
};

// SingleInputNode has one dependency and it sinks data from the dependency.
class SingleInputNode : public SingleDependencyNode {
 public:
  std::unique_ptr<PlanNodeDescription> explain() const override;

  PlanNode* clone() const override {
    DLOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

 protected:
  void cloneMembers(const SingleInputNode& node) {
    SingleDependencyNode::cloneMembers(node);
  }

  void copyInputColNames(const PlanNode* input);

  SingleInputNode(QueryContext* qctx, Kind kind, const PlanNode* dep);
};

// BinaryInputNode has two dependencies and it sinks data from them.
class BinaryInputNode : public PlanNode {
 public:
  void setLeftDep(const PlanNode* left) {
    setDep(0, left);
  }

  void setRightDep(const PlanNode* right) {
    setDep(1, right);
  }

  void setLeftVar(const std::string& leftVar) {
    setInputVar(leftVar, 0);
  }

  void setRightVar(const std::string& rightVar) {
    setInputVar(rightVar, 1);
  }

  const PlanNode* left() const {
    return dep(0);
  }

  const PlanNode* right() const {
    return dep(1);
  }

  const std::string& leftInputVar() const {
    return inputVars_[0]->name;
  }

  const std::string& rightInputVar() const {
    return inputVars_[1]->name;
  }

  PlanNode* clone() const override {
    DLOG(FATAL) << "Shouldn't call the unimplemented method for " << kind_;
    return nullptr;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  BinaryInputNode(QueryContext* qctx, Kind kind, const PlanNode* left, const PlanNode* right);
  BinaryInputNode(QueryContext* qctx, Kind kind);

  void cloneMembers(const BinaryInputNode& node) {
    PlanNode::cloneMembers(node);
  }
};

// some PlanNode may depend on multiple Nodes(may be one OR more)
// Attention: user need to set inputs byself
class VariableDependencyNode : public PlanNode {
 public:
  std::unique_ptr<PlanNodeDescription> explain() const override;

  PlanNode* clone() const override {
    DLOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

 protected:
  VariableDependencyNode(QueryContext* qctx, Kind kind) : PlanNode(qctx, kind) {}

  void cloneMembers(const VariableDependencyNode& node) {
    PlanNode::cloneMembers(node);
  }

  std::vector<int64_t> dependIds() const {
    std::vector<int64_t> ids(numDeps());
    std::transform(dependencies_.begin(), dependencies_.end(), ids.begin(), [](auto& dep) {
      return dep->id();
    });
    return ids;
  }
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_PLAN_PLANNODE_H_
