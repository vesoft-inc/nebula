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
 * The StateTransition tells executor which DCGNode it would transfer to.
 */
class PlanNode;
using StateTransitionTable = std::vector<std::shared_ptr<PlanNode>>;
class StateTransition {
public:
    enum State : int8_t {
        kUnknown = 0,
        kTrue = 1,
        kFalse = 2,
        kConcurrency = 3
    };

    const Expression* expr() const {
        return expr_.get();
    }

    const StateTransitionTable table() const {
        return table_;
    }

private:
    std::unique_ptr<Expression>             expr_;
    StateTransitionTable                    table_;
};

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
class PlanNode {
public:
    enum Kind : uint8_t {
        kUnknown = 0,
        kGetNeighbor,
        kGetVertex,
        kGetEdge,
        kReadIndex,
        kFilter,
        kUnion,
        kIntersect,
        kMinus,
        kProject,
        kSort,
        kLimit,
        kAggregate
    };

    PlanNode(std::vector<std::string>&& colNames, StateTransition&& stateTrans) {
        outputColNames_ = std::move(colNames);
        stateTrans_ = std::move(stateTrans);
    }

    virtual ~PlanNode() = default;

    Kind kind() const {
        return kind_;
    }

    /**
     *  To explain how a query would be executed
     */
    virtual std::string explain() const = 0;

    std::vector<std::string> outputCols() const {
        return outputColNames_;
    }

    /**
     *  Execution engine will calculate the state by this expression.
     */
    const Expression* stateTransExpr() {
        return stateTrans_.expr();
    }

    /**
     *  This table is used for finding the next node(s) to be executed.
     */
    const StateTransitionTable table() {
        return stateTrans_.table();
    }

protected:
    Kind                        kind_{Kind::kUnknown};
    std::vector<std::string>    outputColNames_;
    StateTransition             stateTrans_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
