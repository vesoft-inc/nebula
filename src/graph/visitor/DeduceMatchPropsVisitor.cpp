/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/DeduceMatchPropsVisitor.h"

#include <sstream>

#include "common/expression/Expression.h"
#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

// visitor
DeduceMatchPropsVisitor::DeduceMatchPropsVisitor(const QueryContext *qctx,
                                                 GraphSpaceID space,
                                                 PropertyTracker &propsUsed,
                                                 const std::string &entityAlias)
    : qctx_(qctx), space_(space), propsUsed_(propsUsed), entityAlias_(entityAlias) {
  DCHECK(qctx != nullptr);
}

void DeduceMatchPropsVisitor::visit(TagPropertyExpression *expr) {
  auto &tagName = expr->sym();
  auto &propName = expr->prop();
  auto ret = qctx_->schemaMng()->toTagID(space_, tagName);
  if (!ret.ok()) {
    status_ = std::move(ret).status();
    return;
  }
  auto tagId = ret.value();
  propsUsed_.vertexPropsMap[entityAlias_][tagId].emplace(propName);
}

void DeduceMatchPropsVisitor::visit(EdgePropertyExpression *expr) {
  auto &edgeName = expr->sym();
  auto &propName = expr->prop();
  auto ret = qctx_->schemaMng()->toEdgeType(space_, edgeName);
  if (!ret.ok()) {
    status_ = std::move(ret).status();
    return;
  }
  auto edgeType = ret.value();
  propsUsed_.edgePropsMap[entityAlias_][edgeType].emplace(propName);
}

void DeduceMatchPropsVisitor::visit(LabelTagPropertyExpression *expr) {
  auto status = qctx_->schemaMng()->toTagID(space_, expr->sym());
  if (!status.ok()) {
    status_ = std::move(status).status();
    return;
  }
  auto &nodeAlias = static_cast<VariablePropertyExpression *>(expr->label())->prop();
  auto &tagName = expr->sym();
  auto &propName = expr->prop();
  auto ret = qctx_->schemaMng()->toTagID(space_, tagName);
  if (!ret.ok()) {
    status_ = std::move(ret).status();
    return;
  }
  auto tagId = ret.value();
  propsUsed_.vertexPropsMap[nodeAlias][tagId].emplace(propName);
}

void DeduceMatchPropsVisitor::visit(InputPropertyExpression *expr) {
  auto &colName = expr->prop();
  propsUsed_.colsSet.emplace(colName);
}

void DeduceMatchPropsVisitor::visit(VariablePropertyExpression *expr) {
  auto &colName = expr->prop();
  propsUsed_.colsSet.emplace(colName);
}

// void DeduceMatchPropsVisitor::visit(AttributeExpression *expr) {
//   auto *lhs = expr->left();
//   auto *rhs = expr->right();
//   if (rhs->kind() != Expression::Kind::kConstant) {
//     return;
//   }
//   auto *constExpr = static_cast<ConstantExpression *>(rhs);
//   auto &propName = constExpr->value();
//   switch (lhs->kind()) {
//     case Expression::Kind::kVarProperty: {  // $e.name
//       auto *varPropExpr = static_cast<VariablePropertyExpression *>(lhs);
//       auto &edgeAlias = varPropExpr->prop();
//       propsUsed_.edgePropsMap[edgeAlias][0].emplace(propName);
//       break;
//     }
//     case Expression::Kind::kInputProperty: {
//       auto *inputPropExpr = static_cast<InputPropertyExpression *>(lhs);
//       auto &edgeAlias = inputPropExpr->prop();
//       propsUsed_.edgePropsMap[edgeAlias][0].emplace(propName);
//       break;
//     }
//     case Expression::Kind::kSubscript: {  // $-.e[0].name
//       auto *subscriptExpr = static_cast<SubscriptExpression *>(lhs);
//       auto *subLeftExpr = subscriptExpr->left();
//       if (subLeftExpr->kind() == Expression::Kind::kInputProperty) {
//         auto *inputPropExpr = static_cast<InputPropertyExpression *>(subLeftExpr);
//         auto &edgeAlias = inputPropExpr->prop();
//         propsUsed_.edgePropsMap[edgeAlias][0].emplace(propName);
//       }
//     }
//     default:
//       break;
//   }
// }

void DeduceMatchPropsVisitor::visit(FunctionCallExpression *expr) {
  auto funName = expr->name();
  if (funName == "id" || funName == "src" || funName == "dst") {
    return;
  }
  for (const auto &arg : expr->args()->args()) {
    arg->accept(this);
    if (!ok()) {
      break;
    }
  }
}

void DeduceMatchPropsVisitor::visit(DestPropertyExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(SourcePropertyExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(EdgeSrcIdExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(EdgeTypeExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(EdgeRankExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(EdgeDstIdExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(UUIDExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(VariableExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(VersionedVariableExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(LabelExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(LabelAttributeExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(ConstantExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(ColumnExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(VertexExpression *expr) {
  UNUSED(expr);
}

void DeduceMatchPropsVisitor::visit(EdgeExpression *expr) {
  UNUSED(expr);
}

}  // namespace graph
}  // namespace nebula
