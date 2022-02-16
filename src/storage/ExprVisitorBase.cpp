/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/ExprVisitorBase.h"

#include <vector>  // for vector

#include "common/base/Base.h"                               // for UNUSED
#include "common/expression/AggregateExpression.h"          // for Aggregate...
#include "common/expression/ArithmeticExpression.h"         // for Arithmeti...
#include "common/expression/AttributeExpression.h"          // for Attribute...
#include "common/expression/ColumnExpression.h"             // for ColumnExp...
#include "common/expression/ContainerExpression.h"          // for ListExpre...
#include "common/expression/FunctionCallExpression.h"       // for ArgumentList
#include "common/expression/LabelAttributeExpression.h"     // for LabelAttr...
#include "common/expression/LabelExpression.h"              // for LabelExpr...
#include "common/expression/ListComprehensionExpression.h"  // for ListCompr...
#include "common/expression/LogicalExpression.h"            // for LogicalEx...
#include "common/expression/PathBuildExpression.h"          // for PathBuild...
#include "common/expression/PredicateExpression.h"          // for Predicate...
#include "common/expression/ReduceExpression.h"             // for ReduceExp...
#include "common/expression/RelationalExpression.h"         // for Relationa...
#include "common/expression/SubscriptExpression.h"          // for Subscript...
#include "common/expression/TypeCastingExpression.h"        // for TypeCasti...
#include "common/expression/UnaryExpression.h"              // for UnaryExpr...
#include "common/expression/VariableExpression.h"           // for VariableE...

namespace nebula {
class CaseExpression;
class ConstantExpression;
class DestPropertyExpression;
class EdgeDstIdExpression;
class EdgeExpression;
class EdgePropertyExpression;
class EdgeRankExpression;
class EdgeSrcIdExpression;
class EdgeTypeExpression;
class InputPropertyExpression;
class LabelTagPropertyExpression;
class SourcePropertyExpression;
class TagPropertyExpression;
class UUIDExpression;
class VariablePropertyExpression;
class VertexExpression;

class CaseExpression;
class ConstantExpression;
class DestPropertyExpression;
class EdgeDstIdExpression;
class EdgeExpression;
class EdgePropertyExpression;
class EdgeRankExpression;
class EdgeSrcIdExpression;
class EdgeTypeExpression;
class InputPropertyExpression;
class LabelTagPropertyExpression;
class SourcePropertyExpression;
class TagPropertyExpression;
class UUIDExpression;
class VariablePropertyExpression;
class VertexExpression;

namespace storage {
void ExprVisitorBase::visit(ConstantExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(UnaryExpression *expr) {
  expr->operand()->accept(this);
}
void ExprVisitorBase::visit(TypeCastingExpression *expr) {
  expr->operand()->accept(this);
}
void ExprVisitorBase::visit(LabelExpression *expr) {
  fatal(expr);
}
void ExprVisitorBase::visit(LabelAttributeExpression *expr) {
  fatal(expr);
}
// binary expression
void ExprVisitorBase::visit(ArithmeticExpression *expr) {
  expr->left()->accept(this);
  expr->right()->accept(this);
}
void ExprVisitorBase::visit(RelationalExpression *expr) {
  expr->left()->accept(this);
  expr->right()->accept(this);
}
void ExprVisitorBase::visit(SubscriptExpression *expr) {
  expr->left()->accept(this);
  expr->right()->accept(this);
}
void ExprVisitorBase::visit(AttributeExpression *expr) {
  fatal(expr);
}
void ExprVisitorBase::visit(LogicalExpression *expr) {
  for (auto operand : expr->operands()) {
    operand->accept(this);
  }
}
// function call
void ExprVisitorBase::visit(FunctionCallExpression *expr) {
  for (auto arg : expr->args()->args()) {
    arg->accept(this);
  }
}
void ExprVisitorBase::visit(AggregateExpression *expr) {
  fatal(expr);
}
void ExprVisitorBase::visit(UUIDExpression *expr) {
  UNUSED(expr);
}
// variable expression
void ExprVisitorBase::visit(VariableExpression *expr) {
  fatal(expr);
}
void ExprVisitorBase::visit(VersionedVariableExpression *expr) {
  fatal(expr);
}
// container expression
void ExprVisitorBase::visit(ListExpression *expr) {
  for (auto item : expr->items()) {
    item->accept(this);
  }
}
void ExprVisitorBase::visit(SetExpression *expr) {
  for (auto item : expr->items()) {
    item->accept(this);
  }
}
void ExprVisitorBase::visit(MapExpression *expr) {
  UNUSED(expr);
}
// property Expression
void ExprVisitorBase::visit(TagPropertyExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(EdgePropertyExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(InputPropertyExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(VariablePropertyExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(DestPropertyExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(SourcePropertyExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(EdgeSrcIdExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(EdgeTypeExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(EdgeRankExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(EdgeDstIdExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(LabelTagPropertyExpression *expr) {
  UNUSED(expr);
}
// vertex/edge expression
void ExprVisitorBase::visit(VertexExpression *expr) {
  UNUSED(expr);
}
void ExprVisitorBase::visit(EdgeExpression *expr) {
  UNUSED(expr);
}
// case expression
void ExprVisitorBase::visit(CaseExpression *expr) {
  UNUSED(expr);
}
// path build expression
void ExprVisitorBase::visit(PathBuildExpression *expr) {
  fatal(expr);
}
// column expression
void ExprVisitorBase::visit(ColumnExpression *expr) {
  fatal(expr);
}
// predicate expression
void ExprVisitorBase::visit(PredicateExpression *expr) {
  fatal(expr);
}
// list comprehension expression
void ExprVisitorBase::visit(ListComprehensionExpression *expr) {
  fatal(expr);
}
// reduce expression
void ExprVisitorBase::visit(ReduceExpression *expr) {
  fatal(expr);
}
// subscript range expression
void ExprVisitorBase::visit(SubscriptRangeExpression *expr) {
  fatal(expr);
}

}  // namespace storage
}  // namespace nebula
