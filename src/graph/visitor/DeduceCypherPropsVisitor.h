/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <unordered_set>

#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/visitor/CypherProps.h"
#include "graph/visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class DeduceCypherPropsVisitor : public ExprVisitorImpl {
 public:
  DeduceCypherPropsVisitor(CypherProps& vertexEdgeProps,
                           const std::unordered_map<std::string, AliasType>& aliases)
      : cypherProps_(vertexEdgeProps), aliases_(aliases) {}

  bool ok() const override { return status_.ok(); }

  const Status& status() const { return status_; }

 private:
  using ExprVisitorImpl::visit;
  void visit(LabelExpression* expr) override;
  void visit(LabelAttributeExpression* expr) override;
  void visit(FunctionCallExpression* expr) override;
  void visit(EdgePropertyExpression* expr) override;
  void visit(TagPropertyExpression* expr) override;
  void visit(InputPropertyExpression* expr) override;
  void visit(VariablePropertyExpression* expr) override;
  void visit(SourcePropertyExpression* expr) override;
  void visit(DestPropertyExpression* expr) override;
  void visit(EdgeSrcIdExpression* expr) override;
  void visit(EdgeTypeExpression* expr) override;
  void visit(EdgeRankExpression* expr) override;
  void visit(EdgeDstIdExpression* expr) override;
  void visit(UUIDExpression* expr) override;
  void visit(VariableExpression* expr) override;
  void visit(VersionedVariableExpression* expr) override;
  void visit(AttributeExpression* expr) override;
  void visit(ConstantExpression* expr) override;
  void visit(VertexExpression* expr) override;
  void visit(EdgeExpression* expr) override;
  void visit(ColumnExpression* expr) override;

  void reportError(const Expression* expr);

  CypherProps& cypherProps_;
  const std::unordered_map<std::string, AliasType>& aliases_;
  Status status_;
};

}  // namespace graph
}  // namespace nebula
