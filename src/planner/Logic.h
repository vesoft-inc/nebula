/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_LOGIC_H_
#define PLANNER_LOGIC_H_

#include "context/QueryContext.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

class StartNode final : public PlanNode {
public:
    static StartNode* make(QueryContext* qctx) {
        return qctx->objPool()->add(new StartNode(qctx->genId()));
    }

private:
    explicit StartNode(int64_t id) : PlanNode(id, Kind::kStart) {}
};

class BinarySelect : public SingleInputNode {
public:
    Expression* condition() const {
        return condition_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    BinarySelect(int64_t id, Kind kind, PlanNode* input, Expression* condition)
        : SingleInputNode(id, kind, input), condition_(condition) {}

    Expression* condition_{nullptr};
};

class Select final : public BinarySelect {
public:
    static Select* make(QueryContext* qctx,
                        PlanNode* input,
                        PlanNode* ifBranch,
                        PlanNode* elseBranch,
                        Expression* condition) {
        return qctx->objPool()->add(
            new Select(qctx->genId(), input, ifBranch, elseBranch, condition));
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
    Select(int64_t id,
           PlanNode* input,
           PlanNode* ifBranch,
           PlanNode* elseBranch,
           Expression* condition)
        : BinarySelect(id, Kind::kSelect, input, condition), if_(ifBranch), else_(elseBranch) {}

private:
    PlanNode* if_{nullptr};
    PlanNode* else_{nullptr};
};

class Loop final : public BinarySelect {
public:
    static Loop* make(QueryContext* qctx, PlanNode* input, PlanNode* body, Expression* condition) {
        return qctx->objPool()->add(new Loop(qctx->genId(), input, body, condition));
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    const PlanNode* body() const {
        return body_;
    }

private:
    Loop(int64_t id, PlanNode* input, PlanNode* body, Expression* condition);

    PlanNode* body_{nullptr};
};

/**
 * This operator is used for pass through situation.
 */
class PassThroughNode final : public SingleInputNode {
public:
    static PassThroughNode* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new PassThroughNode(qctx->genId(), input));
    }

private:
    PassThroughNode(int64_t id, PlanNode* input) : SingleInputNode(id, Kind::kPassThrough, input) {}
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_LOGIC_H_
