/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLAN_LOGIC_H_
#define GRAPH_PLANNER_PLAN_LOGIC_H_

#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

// StartNode is a spetial leaf node that helps the scheduler to work properly.
class StartNode final : public PlanNode {
 public:
  static StartNode* make(QueryContext* qctx) {
    return qctx->objPool()->makeAndAdd<StartNode>(qctx);
  }

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  explicit StartNode(QueryContext* qctx) : PlanNode(qctx, Kind::kStart) {}

  void cloneMembers(const StartNode&);
};

class BinarySelect : public SingleInputNode {
 public:
  Expression* condition() const {
    return condition_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  BinarySelect(QueryContext* qctx, Kind kind, PlanNode* input, Expression* condition)
      : SingleInputNode(qctx, kind, input), condition_(condition) {}

  void cloneMembers(const BinarySelect& s) {
    SingleInputNode::cloneMembers(s);

    condition_ = s.condition();
  }

 protected:
  Expression* condition_{nullptr};
};

// Select the if branch or else branch at runtime
class Select final : public BinarySelect {
 public:
  static Select* make(QueryContext* qctx,
                      PlanNode* input,
                      PlanNode* ifBranch = nullptr,
                      PlanNode* elseBranch = nullptr,
                      Expression* condition = nullptr) {
    return qctx->objPool()->makeAndAdd<Select>(qctx, input, ifBranch, elseBranch, condition);
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
  friend ObjectPool;
  Select(QueryContext* qctx,
         PlanNode* input,
         PlanNode* ifBranch,
         PlanNode* elseBranch,
         Expression* condition)
      : BinarySelect(qctx, Kind::kSelect, input, condition), if_(ifBranch), else_(elseBranch) {}

  void cloneMembers(const Select&);

 private:
  PlanNode* if_{nullptr};
  PlanNode* else_{nullptr};
};

// Executing the branch multi times at runtime.
class Loop final : public BinarySelect {
 public:
  static Loop* make(QueryContext* qctx,
                    PlanNode* input,
                    PlanNode* body = nullptr,
                    Expression* condition = nullptr) {
    return qctx->objPool()->makeAndAdd<Loop>(qctx, input, body, condition);
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
  friend ObjectPool;
  Loop(QueryContext* qctx, PlanNode* input, PlanNode* body, Expression* condition)
      : BinarySelect(qctx, Kind::kLoop, input, condition), body_(body) {}

  void cloneMembers(const Loop&);

 private:
  PlanNode* body_{nullptr};
};

// This operator is used for pass through situation.
class PassThroughNode final : public SingleInputNode {
 public:
  static PassThroughNode* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<PassThroughNode>(qctx, input);
  }

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  PassThroughNode(QueryContext* qctx, PlanNode* input)
      : SingleInputNode(qctx, Kind::kPassThrough, input) {}

  void cloneMembers(const PassThroughNode&);
};

// This operator is used for getting a named alias from another executed operator.
class Argument final : public SingleInputNode {
 public:
  static Argument* make(QueryContext* qctx, std::string alias, const PlanNode* dep = nullptr) {
    return qctx->objPool()->makeAndAdd<Argument>(qctx, alias, dep);
  }

  PlanNode* clone() const override;

  const std::string& getAlias() const {
    return alias_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  Argument(QueryContext* qctx, std::string alias, const PlanNode* dep = nullptr);

  void cloneMembers(const Argument&);

 private:
  std::string alias_;
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_PLAN_LOGIC_H_
