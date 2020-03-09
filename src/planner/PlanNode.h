/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {
namespace graph {

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
class StartNode;
class PlanNode {
public:
    enum class Kind : uint8_t {
        kUnknown = 0,
        kStart,
        kGetNeighbors,
        kGetVertices,
        kGetEdges,
        kReadIndex,
        kFilter,
        kUnion,
        kIntersect,
        kMinus,
        kProject,
        kSort,
        kLimit,
        kAggregate,
        kSelector,
        kLoop,
        kBuildShortestPath,
        kRegisterVariable,
        kRegisterSpaceToSession,
    };

    PlanNode() = default;

    PlanNode(std::vector<std::string>&& colNames,
             std::vector<std::shared_ptr<PlanNode>>&& children) {
        outputColNames_ = std::move(colNames);
        children_ = std::move(children);
    }

    virtual ~PlanNode() = default;

    /**
     * To explain how a query would be executed
     */
    virtual std::string explain() const = 0;

    Kind kind() const {
        return kind_;
    }

    int64_t id() const {
        return id_;
    }

    std::vector<std::string> outputColNames() const {
        return outputColNames_;
    }

    const std::vector<std::shared_ptr<PlanNode>>& children() const {
        return children_;
    }

    void setId(int64_t id) {
        id_ = id;
    }

    void setOutputColNames(std::vector<std::string>&& cols) {
        outputColNames_ = std::move(cols);
    }

    void setChildren(std::vector<std::shared_ptr<PlanNode>>&& children) {
        children_ = std::move(children);
    }

    /**
     * Append a sub-plan to another one.
     */
    Status append(std::shared_ptr<PlanNode> start);

    /**
     * Merge two sub-plan.
     */
    Status merge(std::shared_ptr<StartNode> start);

protected:
    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{-1};
    std::vector<std::string>                 outputColNames_;
    std::vector<std::shared_ptr<PlanNode>>   children_;
};

/**
 * An execution plan will start from a StartNode.
 */
class StartNode final : public PlanNode {
public:
    StartNode() {
        kind_ = PlanNode::Kind::kStart;
    }

    StartNode(std::vector<std::string>&& colNames,
              std::vector<std::shared_ptr<PlanNode>>&& children)
              : PlanNode(std::move(colNames), std::move(children)) {
        kind_ = PlanNode::Kind::kStart;
    }

    std::string explain() const override {
        return "Start";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
