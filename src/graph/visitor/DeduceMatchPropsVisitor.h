/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_DEDUCEMATCHPROPSVISITOR_H
#define GRAPH_VISITOR_DEDUCEMATCHPROPSVISITOR_H

#include "common/base/Status.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/thrift/ThriftTypes.h"
#include "graph/visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class QueryContext;

class DeduceMatchPropsVisitor : public ExprVisitorImpl {
 public:
  DeduceMatchPropsVisitor(const QueryContext* qctx,
                          GraphSpaceID space,
                          PropertyTracker& propsUsed,
                          const std::string& entityAlias);

  bool ok() const override {
    return status_.ok();
  }

  const Status& status() const {
    return status_;
  }

 private:
  using ExprVisitorImpl::visit;
  void visit(TagPropertyExpression* expr) override;
  void visit(EdgePropertyExpression* expr) override;
  void visit(LabelTagPropertyExpression* expr) override;
  void visit(InputPropertyExpression* expr) override;
  void visit(VariablePropertyExpression* expr) override;
  // void visit(AttributeExpression* expr) override;
  void visit(FunctionCallExpression* expr) override;

  void visit(SourcePropertyExpression* expr) override;
  void visit(DestPropertyExpression* expr) override;
  void visit(EdgeSrcIdExpression* expr) override;
  void visit(EdgeTypeExpression* expr) override;
  void visit(EdgeRankExpression* expr) override;
  void visit(EdgeDstIdExpression* expr) override;
  void visit(UUIDExpression* expr) override;
  void visit(VariableExpression* expr) override;
  void visit(VersionedVariableExpression* expr) override;
  void visit(LabelExpression* expr) override;
  void visit(LabelAttributeExpression* expr) override;
  void visit(ConstantExpression* expr) override;
  void visit(VertexExpression* expr) override;
  void visit(EdgeExpression* expr) override;
  void visit(ColumnExpression* expr) override;

  const QueryContext* qctx_{nullptr};
  GraphSpaceID space_;
  PropertyTracker& propsUsed_;
  std::string entityAlias_;

  Status status_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_DEDUCEMATCHPROPSVISITOR_H
