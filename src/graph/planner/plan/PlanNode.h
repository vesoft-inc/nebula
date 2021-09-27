/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_PLAN_PLANNODE_H_
#define GRAPH_PLANNER_PLAN_PLANNODE_H_

#include "common/expression/Expression.h"
#include "common/graph/Response.h"
#include "graph/context/QueryContext.h"
#include "graph/context/Symbols.h"

namespace nebula {
namespace graph {

static inline constexpr uint64_t kindGen(const uint64_t main, const uint64_t variant) {
  return (main << 48) | (variant & 0x0000FFFFFFFFFFFF);
}

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
class PlanNode {
 public:
  // Single Level Polymorphism
  // Encode the kind with format: |16Bits for main type|48Bits for variant type|
  // The variants must inherit the main type, this will be checked by PlanNode::asNode method,
  // so don't cast node by other ways
  // And the main type and variant type also means logical node and physical node in future
  enum class Kind : uint64_t {
    kUnknown = kindGen(0, 0),

    // Query
    kGetNeighbors = kindGen(1, 0),
    kGetVertices = kindGen(2, 0),
    kGetEdges = kindGen(3, 0),
    // ------------------
    // TODO(yee): refactor in logical plan
    kIndexScan = kindGen(4, 0),
    kTagIndexFullScan = kindGen(4, 1),
    kTagIndexPrefixScan = kindGen(4, 2),
    kTagIndexRangeScan = kindGen(4, 3),
    kEdgeIndexFullScan = kindGen(4, 4),
    kEdgeIndexPrefixScan = kindGen(4, 5),
    kEdgeIndexRangeScan = kindGen(4, 6),
    // ------------------
    kFilter = kindGen(5, 0),
    kUnion = kindGen(6, 0),
    kUnionAllVersionVar = kindGen(7, 0),
    kIntersect = kindGen(8, 0),
    kMinus = kindGen(9, 0),
    kProject = kindGen(10, 0),
    kUnwind = kindGen(11, 0),
    kSort = kindGen(12, 0),
    kTopN = kindGen(13, 0),
    kLimit = kindGen(14, 0),
    kAggregate = kindGen(15, 0),
    kDedup = kindGen(16, 0),
    kAssign = kindGen(17, 0),
    kBFSShortest = kindGen(18, 0),
    kProduceSemiShortestPath = kindGen(19, 0),
    kConjunctPath = kindGen(20, 0),
    kProduceAllPaths = kindGen(21, 0),
    kCartesianProduct = kindGen(22, 0),
    kSubgraph = kindGen(23, 0),
    kDataCollect = kindGen(24, 0),
    kLeftJoin = kindGen(25, 0),
    kInnerJoin = kindGen(26, 0),

    // Logic
    kStart = kindGen(27, 0),
    kSelect = kindGen(28, 0),
    kLoop = kindGen(29, 0),
    kPassThrough = kindGen(30, 0),

    // schema related
    kCreateSpace = kindGen(31, 0),
    kCreateTag = kindGen(32, 0),
    kCreateEdge = kindGen(33, 0),
    kDescSpace = kindGen(34, 0),
    kShowCreateSpace = kindGen(35, 0),
    kDescTag = kindGen(36, 0),
    kDescEdge = kindGen(37, 0),
    kAlterTag = kindGen(38, 0),
    kAlterEdge = kindGen(39, 0),
    kShowSpaces = kindGen(40, 0),
    kSwitchSpace = kindGen(41, 0),
    kShowTags = kindGen(42, 0),
    kShowEdges = kindGen(43, 0),
    kShowCreateTag = kindGen(44, 0),
    kShowCreateEdge = kindGen(45, 0),
    kDropSpace = kindGen(46, 0),
    kDropTag = kindGen(47, 0),
    kDropEdge = kindGen(48, 0),

    // index related
    kCreateTagIndex = kindGen(49, 0),
    kCreateEdgeIndex = kindGen(50, 0),
    kCreateFTIndex = kindGen(51, 0),
    kDropFTIndex = kindGen(52, 0),
    kDropTagIndex = kindGen(53, 0),
    kDropEdgeIndex = kindGen(54, 0),
    kDescTagIndex = kindGen(55, 0),
    kDescEdgeIndex = kindGen(56, 0),
    kShowCreateTagIndex = kindGen(57, 0),
    kShowCreateEdgeIndex = kindGen(58, 0),
    kShowTagIndexes = kindGen(59, 0),
    kShowEdgeIndexes = kindGen(60, 0),
    kShowTagIndexStatus = kindGen(61, 0),
    kShowEdgeIndexStatus = kindGen(62, 0),
    kInsertVertices = kindGen(63, 0),
    kInsertEdges = kindGen(64, 0),
    kBalanceLeaders = kindGen(65, 0),
    kBalance = kindGen(66, 0),
    kStopBalance = kindGen(67, 0),
    kResetBalance = kindGen(68, 0),
    kShowBalance = kindGen(69, 0),
    kSubmitJob = kindGen(70, 0),
    kShowHosts = kindGen(71, 0),

    // user related
    kCreateUser = kindGen(72, 0),
    kDropUser = kindGen(73, 0),
    kUpdateUser = kindGen(74, 0),
    kGrantRole = kindGen(75, 0),
    kRevokeRole = kindGen(76, 0),
    kChangePassword = kindGen(77, 0),
    kListUserRoles = kindGen(78, 0),
    kListUsers = kindGen(79, 0),
    kListRoles = kindGen(80, 0),

    // Snapshot
    kCreateSnapshot = kindGen(81, 0),
    kDropSnapshot = kindGen(82, 0),
    kShowSnapshots = kindGen(83, 0),

    // Update/Delete
    kDeleteVertices = kindGen(84, 0),
    kDeleteEdges = kindGen(85, 0),
    kUpdateVertex = kindGen(86, 0),
    kDeleteTags = kindGen(87, 0),
    kUpdateEdge = kindGen(88, 0),

    // Show
    kShowParts = kindGen(89, 0),
    kShowCharset = kindGen(90, 0),
    kShowCollation = kindGen(91, 0),
    kShowStats = kindGen(92, 0),
    kShowConfigs = kindGen(93, 0),
    kSetConfig = kindGen(94, 0),
    kGetConfig = kindGen(95, 0),
    kShowMetaLeader = kindGen(96, 0),

    // zone related
    kShowGroups = kindGen(97, 0),
    kShowZones = kindGen(98, 0),
    kAddGroup = kindGen(99, 0),
    kDropGroup = kindGen(100, 0),
    kDescribeGroup = kindGen(101, 0),
    kAddZoneIntoGroup = kindGen(102, 0),
    kDropZoneFromGroup = kindGen(103, 0),
    kAddZone = kindGen(104, 0),
    kDropZone = kindGen(105, 0),
    kDescribeZone = kindGen(106, 0),
    kAddHostIntoZone = kindGen(107, 0),
    kDropHostFromZone = kindGen(108, 0),

    // listener related
    kAddListener = kindGen(109, 0),
    kRemoveListener = kindGen(110, 0),
    kShowListener = kindGen(111, 0),

    // text service related
    kShowTSClients = kindGen(112, 0),
    kShowFTIndexes = kindGen(113, 0),
    kSignInTSService = kindGen(114, 0),
    kSignOutTSService = kindGen(115, 0),
    kDownload = kindGen(116, 0),
    kIngest = kindGen(117, 0),
    kShowSessions = kindGen(118, 0),
    kUpdateSession = kindGen(119, 0),

    kShowQueries = kindGen(120, 0),
    kKillQuery = kindGen(121, 0),
  };

  template <typename T>
  std::enable_if_t<std::is_base_of<PlanNode, T>::value, const T*> asNode() const {
    DCHECK(dynamic_cast<const T*>(this) != nullptr);
    return static_cast<const T*>(this);
  }

  static uint64_t mainType(Kind kind) { return static_cast<uint64_t>(kind) & 0xFFFF000000000000; }

  static uint64_t variantType(Kind kind) {
    return static_cast<uint64_t>(kind) & 0x0000FFFFFFFFFFFF;
  }

  static bool isMainType(Kind kind) { return variantType(kind) == 0; }

  // Variant X is Variant X
  // Variant X is main type of Variant X
  // Variant X is not Variant Y
  bool isA(Kind kind) const {
    if (kind == kind_) {
      return true;
    }
    if (!isMainType(kind)) {
      return false;
    }
    if (mainType(kind) == mainType(kind_)) {
      return true;
    }
    return false;
  }

  bool isQueryNode() const { return kind_ < Kind::kStart; }

  // Describe plan node
  virtual std::unique_ptr<PlanNodeDescription> explain() const;

  virtual PlanNode* clone() const = 0;

  virtual void calcCost();

  Kind kind() const { return kind_; }

  int64_t id() const { return id_; }

  QueryContext* qctx() const { return qctx_; }

  bool isSingleInput() const { return numDeps() == 1U; }

  void setOutputVar(const std::string& var);

  const std::string& outputVar(size_t index = 0) const { return outputVarPtr(index)->name; }

  Variable* outputVarPtr(size_t index = 0) const {
    DCHECK_LT(index, outputVars_.size());
    return outputVars_[index];
  }

  const std::vector<Variable*>& outputVars() const { return outputVars_; }

  const std::vector<std::string>& colNames() const { return outputVarPtr(0)->colNames; }

  void setId(int64_t id) { id_ = id; }

  void setColNames(std::vector<std::string> cols) { outputVarPtr(0)->colNames = std::move(cols); }

  const auto& dependencies() const { return dependencies_; }

  const PlanNode* dep(size_t index = 0) const {
    DCHECK_LT(index, dependencies_.size());
    return dependencies_.at(index);
  }

  void setDep(size_t index, const PlanNode* dep) {
    DCHECK_LT(index, dependencies_.size());
    dependencies_[index] = DCHECK_NOTNULL(dep);
  }

  void addDep(const PlanNode* dep) { dependencies_.emplace_back(dep); }

  size_t numDeps() const { return dependencies_.size(); }

  std::string inputVar(size_t idx = 0UL) const {
    DCHECK_LT(idx, inputVars_.size());
    return inputVars_[idx] ? inputVars_[idx]->name : "";
  }

  void setInputVar(const std::string& varname, size_t idx = 0UL);

  const std::vector<Variable*>& inputVars() const { return inputVars_; }

  void releaseSymbols();

  static const char* toString(Kind kind);
  std::string toString() const;

  double cost() const { return cost_; }

 protected:
  PlanNode(QueryContext* qctx, Kind kind);

  virtual ~PlanNode() = default;

  static void addDescription(std::string key, std::string value, PlanNodeDescription* desc);

  void readVariable(const std::string& varname);
  void readVariable(Variable* varPtr);
  void cloneMembers(const PlanNode& node) {
    // TODO maybe shall copy cost_ and dependencies_ too
    inputVars_ = node.inputVars_;
    outputVars_ = node.outputVars_;
  }

  QueryContext* qctx_{nullptr};
  Kind kind_{Kind::kUnknown};
  int64_t id_{-1};
  double cost_{0.0};
  std::vector<const PlanNode*> dependencies_;
  std::vector<Variable*> inputVars_;
  std::vector<Variable*> outputVars_;
};

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind);

// Dependencies will cover the inputs, For example bi input require bi
// dependencies as least, but single dependencies may don't need any inputs (I.E
// admin plan node) Single dependecy without input It's useful for admin plan
// node
class SingleDependencyNode : public PlanNode {
 public:
  void dependsOn(const PlanNode* dep) { setDep(0, dep); }

  PlanNode* clone() const override {
    LOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

 protected:
  SingleDependencyNode(QueryContext* qctx, Kind kind, const PlanNode* dep) : PlanNode(qctx, kind) {
    addDep(dep);
  }

  void cloneMembers(const SingleDependencyNode& node) { PlanNode::cloneMembers(node); }

  std::unique_ptr<PlanNodeDescription> explain() const override;
};

class SingleInputNode : public SingleDependencyNode {
 public:
  std::unique_ptr<PlanNodeDescription> explain() const override;

  PlanNode* clone() const override {
    LOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

 protected:
  void cloneMembers(const SingleInputNode& node) { SingleDependencyNode::cloneMembers(node); }

  void copyInputColNames(const PlanNode* input) {
    if (input != nullptr) {
      setColNames(input->colNames());
    }
  }

  SingleInputNode(QueryContext* qctx, Kind kind, const PlanNode* dep);
};

class BinaryInputNode : public PlanNode {
 public:
  void setLeftDep(const PlanNode* left) { setDep(0, left); }

  void setRightDep(const PlanNode* right) { setDep(1, right); }

  void setLeftVar(const std::string& leftVar) { setInputVar(leftVar, 0); }

  void setRightVar(const std::string& rightVar) { setInputVar(rightVar, 1); }

  const PlanNode* left() const { return dep(0); }

  const PlanNode* right() const { return dep(1); }

  const std::string& leftInputVar() const { return inputVars_[0]->name; }

  const std::string& rightInputVar() const { return inputVars_[1]->name; }

  PlanNode* clone() const override {
    LOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  BinaryInputNode(QueryContext* qctx, Kind kind, const PlanNode* left, const PlanNode* right);

  void cloneMembers(const BinaryInputNode& node) { PlanNode::cloneMembers(node); }
};

// some PlanNode may depend on multiple Nodes(may be one OR more)
// Attention: user need to set inputs byself
class VariableDependencyNode : public PlanNode {
 public:
  std::unique_ptr<PlanNodeDescription> explain() const override;

  PlanNode* clone() const override {
    LOG(FATAL) << "Shouldn't call the unimplemented method";
    return nullptr;
  }

 protected:
  VariableDependencyNode(QueryContext* qctx, Kind kind) : PlanNode(qctx, kind) {}

  void cloneMembers(const VariableDependencyNode& node) { PlanNode::cloneMembers(node); }

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
