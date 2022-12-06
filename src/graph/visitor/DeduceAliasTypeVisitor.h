/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef NEBULA_GRAPH_DEDUCEALIASTYPEVISITOR_H
#define NEBULA_GRAPH_DEDUCEALIASTYPEVISITOR_H

#include "common/base/Status.h"
#include "common/datatypes/Value.h"
#include "common/expression/ExprVisitor.h"
#include "graph/context/ValidateContext.h"
#include "graph/context/ast/CypherAstContext.h"

namespace nebula {
namespace graph {

class QueryContext;

// An expression visitor enable deducing AliasType when possible.
class DeduceAliasTypeVisitor final : public ExprVisitor {
 public:
  DeduceAliasTypeVisitor(QueryContext *qctx,
                         ValidateContext *vctx,
                         GraphSpaceID space,
                         AliasType inputType);

  ~DeduceAliasTypeVisitor() = default;

  bool ok() const {
    return status_.ok();
  }

  Status status() && {
    return std::move(status_);
  }

  AliasType outputType() const {
    return outputType_;
  }

 private:
  // Most expression cannot be used to deducing,
  // or deduced to primitive types, use the default kRuntime type
  void visit(ConstantExpression *) override {}
  void visit(UnaryExpression *) override {}
  void visit(TypeCastingExpression *) override {}
  void visit(LabelExpression *) override {}
  void visit(LabelAttributeExpression *) override {}
  void visit(ArithmeticExpression *) override {}
  void visit(RelationalExpression *) override {}
  void visit(AttributeExpression *) override {}
  void visit(LogicalExpression *) override {}
  void visit(AggregateExpression *) override {}
  void visit(UUIDExpression *) override {}
  void visit(VariableExpression *) override {}
  void visit(VersionedVariableExpression *) override {}
  void visit(ListExpression *) override {}
  void visit(SetExpression *) override {}
  void visit(MapExpression *) override {}
  void visit(LabelTagPropertyExpression *) override {}
  void visit(TagPropertyExpression *) override {}
  void visit(EdgePropertyExpression *) override {}
  void visit(InputPropertyExpression *) override {}
  void visit(VariablePropertyExpression *) override {}
  void visit(DestPropertyExpression *) override {}
  void visit(SourcePropertyExpression *) override {}
  void visit(EdgeSrcIdExpression *) override {}
  void visit(EdgeTypeExpression *) override {}
  void visit(EdgeRankExpression *) override {}
  void visit(EdgeDstIdExpression *) override {}
  void visit(CaseExpression *) override {}
  void visit(ColumnExpression *) override {}
  void visit(PredicateExpression *) override {}
  void visit(ListComprehensionExpression *) override {}
  void visit(ReduceExpression *) override {}
  void visit(MatchPathPatternExpression *) override {}

  // Expression may have deducing potential
  void visit(VertexExpression *expr) override;
  void visit(EdgeExpression *expr) override;
  void visit(PathBuildExpression *expr) override;
  void visit(FunctionCallExpression *expr) override;
  void visit(SubscriptExpression *expr) override;
  void visit(SubscriptRangeExpression *expr) override;

 private:
  QueryContext *qctx_{nullptr};
  ValidateContext *vctx_{nullptr};
  GraphSpaceID space_;
  Status status_;
  AliasType inputType_{AliasType::kRuntime};
  // Default output type is Runtime
  AliasType outputType_{AliasType::kRuntime};
};

}  // namespace graph
}  // namespace nebula

#endif  // NEBULA_GRAPH_DEDUCEALIASTYPEVISITOR_H
