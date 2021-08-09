/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_LOGIC_H_
#define PLANNER_PLAN_LOGIC_H_

#include "planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

class StartNode final : public PlanNode {
public:
    static StartNode* make(QueryContext* qctx) {
        return qctx->objPool()->add(new StartNode(qctx));
    }

    PlanNode* clone() const override;

private:
    explicit StartNode(QueryContext* qctx)
        : PlanNode(qctx, Kind::kStart) {}

    void cloneMembers(const StartNode&);
};

class BinarySelect : public SingleInputNode {
public:
    Expression* condition() const {
        return condition_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    BinarySelect(QueryContext* qctx,
                 Kind kind,
                 PlanNode* input,
                 Expression* condition)
        : SingleInputNode(qctx, kind, input), condition_(condition) {}

    void cloneMembers(const BinarySelect& s) {
        SingleInputNode::cloneMembers(s);

        condition_ = s.condition();
    }

protected:
    Expression* condition_{nullptr};
};

class Select final : public BinarySelect {
public:
    static Select* make(QueryContext* qctx,
                        PlanNode* input,
                        PlanNode* ifBranch = nullptr,
                        PlanNode* elseBranch = nullptr,
                        Expression* condition = nullptr) {
        return qctx->objPool()->add(new Select(qctx, input, ifBranch, elseBranch, condition));
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

    std::unique_ptr<PlanNodeDescription> explain() const override;

    PlanNode* clone() const override;

private:
    Select(QueryContext* qctx,
           PlanNode* input,
           PlanNode* ifBranch,
           PlanNode* elseBranch,
           Expression* condition)
        : BinarySelect(qctx, Kind::kSelect, input, condition),
          if_(ifBranch),
          else_(elseBranch) {}

    void cloneMembers(const Select&);

private:
    PlanNode* if_{nullptr};
    PlanNode* else_{nullptr};
};

class Loop final : public BinarySelect {
public:
    static Loop* make(QueryContext* qctx,
                      PlanNode* input,
                      PlanNode* body = nullptr,
                      Expression* condition = nullptr) {
        return qctx->objPool()->add(new Loop(qctx, input, body, condition));
    }

    void setBody(PlanNode* body) {
        body_ = body;
    }

    const PlanNode* body() const {
        return body_;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    PlanNode* clone() const override;

private:
    Loop(QueryContext* qctx, PlanNode* input, PlanNode* body, Expression* condition)
        : BinarySelect(qctx, Kind::kLoop, input, condition), body_(body) {}

    void cloneMembers(const Loop&);

private:
    PlanNode* body_{nullptr};
};

/**
 * This operator is used for pass through situation.
 */
class PassThroughNode final : public SingleInputNode {
public:
    static PassThroughNode* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new PassThroughNode(qctx, input));
    }

    PlanNode* clone() const override;

private:
    PassThroughNode(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kPassThrough, input) {}

    void cloneMembers(const PassThroughNode&);
};
}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_PLAN_LOGIC_H_
