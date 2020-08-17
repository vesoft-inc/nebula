/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

namespace cpp2 {
class PlanNodeDescription;
}   // namespace cpp2

class ExecutionPlan;

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
class PlanNode {
public:
    enum class Kind : uint8_t {
        kUnknown = 0,
        kStart,
        kGetNeighbors,
        kGetVertices,
        kGetEdges,
        kIndexScan,
        kFilter,
        kUnion,
        kIntersect,
        kMinus,
        kProject,
        kSort,
        kLimit,
        kAggregate,
        kSelect,
        kLoop,
        kSwitchSpace,
        kDedup,
        kMultiOutputs,
        kCreateSpace,
        kCreateTag,
        kCreateEdge,
        kDescSpace,
        kShowCreateSpace,
        kDescTag,
        kDescEdge,
        kAlterTag,
        kAlterEdge,
        kShowSpaces,
        kShowTags,
        kShowEdges,
        kShowCreateTag,
        kShowCreateEdge,
        kDropSpace,
        kDropTag,
        kDropEdge,
        kInsertVertices,
        kInsertEdges,
        kSubmitJob,
        kShowHosts,
        kDataCollect,
        kCreateSnapshot,
        kDropSnapshot,
        kShowSnapshots,
        kDataJoin,
        kDeleteVertices,
        kDeleteEdges,
        kUpdateVertex,
        kUpdateEdge,
        kShowParts,
        kShowCharset,
        kShowCollation,
        kShowConfigs,
        kSetConfig,
        kGetConfig,
    };

    PlanNode(ExecutionPlan* plan, Kind kind);

    virtual ~PlanNode() = default;

    // Describe plan node
    virtual std::unique_ptr<cpp2::PlanNodeDescription> explain() const;

    Kind kind() const {
        return kind_;
    }

    int64_t id() const {
        return id_;
    }

    void setOutputVar(std::string var) {
        outputVar_ = std::move(var);
    }

    std::string varName() const {
        return outputVar_;
    }

    const ExecutionPlan* plan() const {
        return plan_;
    }

    std::vector<std::string> colNames() const {
        return colNames_;
    }

    const std::vector<std::string>& colNamesRef() const {
        return colNames_;
    }

    void setId(int64_t id) {
        id_ = id;
        outputVar_ = folly::stringPrintf("__%s_%ld", toString(kind_), id_);
    }

    void setPlan(ExecutionPlan* plan) {
        plan_ = plan;
    }

    void setColNames(std::vector<std::string>&& cols) {
        colNames_ = std::move(cols);
    }

    void setColNames(const std::vector<std::string>& cols) {
        colNames_ = cols;
    }

    static const char* toString(Kind kind);

    const std::string& nodeLabel() const {
        return outputVar_;
    }

protected:
    static void addDescription(std::string key, std::string value, cpp2::PlanNodeDescription* desc);

    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{IdGenerator::INVALID_ID};
    ExecutionPlan*                           plan_{nullptr};
    using VariableName = std::string;
    VariableName                             outputVar_;
    std::vector<std::string>                 colNames_;
};

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind);

// Dependencies will cover the inputs, For example bi input require bi dependencies as least,
// but single dependencies may don't need any inputs (I.E admin plan node)
// Single dependecy without input
// It's useful for admin plan node
class SingleDependencyNode : public PlanNode {
public:
    const PlanNode* dep() const {
        return dependency_;
    }

    void dependsOn(PlanNode *dep) {
        dependency_ = DCHECK_NOTNULL(dep);
    }

protected:
    SingleDependencyNode(ExecutionPlan *plan, Kind kind, const PlanNode *dep)
        : PlanNode(plan, kind), dependency_(dep) {}

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const PlanNode *dependency_;
};

class SingleInputNode : public SingleDependencyNode {
public:
    void setInputVar(std::string inputVar) {
        inputVar_ = std::move(inputVar);
    }

    const std::string& inputVar() const {
        return inputVar_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    SingleInputNode(ExecutionPlan* plan, Kind kind, const PlanNode* dep)
        : SingleDependencyNode(plan, kind, dep) {
    }

    // Datasource for this node.
    std::string inputVar_;
};

class BiInputNode : public PlanNode {
public:
    void setLeft(PlanNode* left) {
        left_ = left;
    }

    void setRight(PlanNode* right) {
        right_ = right;
    }

    void setLeftVar(std::string leftVar) {
        leftVar_ = std::move(leftVar);
    }

    void setRightVar(std::string rightVar) {
        rightVar_ = std::move(rightVar);
    }

    const PlanNode* left() const {
        return left_;
    }

    const PlanNode* right() const {
        return right_;
    }

    const std::string& leftInputVar() const {
        return leftVar_;
    }

    const std::string& rightInputVar() const {
        return rightVar_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    BiInputNode(ExecutionPlan* plan, Kind kind, PlanNode* left, PlanNode* right)
        : PlanNode(plan, kind), left_(left), right_(right) {
    }

    PlanNode* left_{nullptr};
    PlanNode* right_{nullptr};
    // Datasource for this node.
    std::string leftVar_;
    std::string rightVar_;
};

}  // namespace graph
}  // namespace nebula

#endif  // PLANNER_PLANNODE_H_
