/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_LOGIC_H_
#define PLANNER_LOGIC_H_

#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

class StartNode final : public PlanNode {
public:
    static StartNode* make(ExecutionPlan* plan) {
        return new StartNode(plan);
    }

private:
    explicit StartNode(ExecutionPlan* plan) : PlanNode(plan, Kind::kStart) {}
};

class BinarySelect : public SingleInputNode {
public:
    Expression* condition() const {
        return condition_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    BinarySelect(ExecutionPlan* plan, Kind kind, PlanNode* input, Expression* condition)
        : SingleInputNode(plan, kind, input), condition_(condition) {}

    Expression* condition_{nullptr};
};

class Select final : public BinarySelect {
public:
    static Select* make(ExecutionPlan* plan,
                        PlanNode* input,
                        PlanNode* ifBranch,
                        PlanNode* elseBranch,
                        Expression* condition) {
        return new Select(plan, input, ifBranch, elseBranch, condition);
    }

    void setIf(PlanNode* ifBranch) {
        if_ = ifBranch;
    }

    void setElse(PlanNode* elseBranch) {
        else_ = elseBranch;
    }

    const PlanNode* then() const {
        return if_;
    }

    const PlanNode* otherwise() const {
        return else_;
    }

private:
    Select(ExecutionPlan* plan,
           PlanNode* input,
           PlanNode* ifBranch,
           PlanNode* elseBranch,
           Expression* condition)
        : BinarySelect(plan, Kind::kSelect, input, condition), if_(ifBranch), else_(elseBranch) {}

private:
    PlanNode* if_{nullptr};
    PlanNode* else_{nullptr};
};

class Loop final : public BinarySelect {
public:
    static Loop* make(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition) {
        return new Loop(plan, input, body, condition);
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    const PlanNode* body() const {
        return body_;
    }

private:
    Loop(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition);

    PlanNode* body_{nullptr};
};

/**
 * This operator is used for multi output situation.
 */
class MultiOutputsNode final : public SingleInputNode {
public:
    static MultiOutputsNode* make(ExecutionPlan* plan, PlanNode* input) {
        return new MultiOutputsNode(input, plan);
    }

private:
    MultiOutputsNode(PlanNode* input, ExecutionPlan* plan)
        : SingleInputNode(plan, Kind::kMultiOutputs, input) {}
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_LOGIC_H_
