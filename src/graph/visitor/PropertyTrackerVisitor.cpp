/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/PropertyTrackerVisitor.h"

#include <sstream>
#include <unordered_set>

#include "common/expression/Expression.h"
#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

PropertyTrackerVisitor::PropertyTrackerVisitor(const QueryContext *qctx,
                                               GraphSpaceID space,
                                               PropertyTracker &propsUsed,
                                               const std::string &entityAlias)
    : qctx_(qctx), space_(space), propsUsed_(propsUsed), entityAlias_(entityAlias) {
  DCHECK(qctx != nullptr);
}

void PropertyTrackerVisitor::visit(TagPropertyExpression *expr) {
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

void PropertyTrackerVisitor::visit(EdgePropertyExpression *expr) {
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

void PropertyTrackerVisitor::visit(LabelTagPropertyExpression *expr) {
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

void PropertyTrackerVisitor::visit(InputPropertyExpression *expr) {
  auto &colName = expr->prop();
  propsUsed_.colsSet.emplace(colName);
}

void PropertyTrackerVisitor::visit(VariablePropertyExpression *expr) {
  auto &colName = expr->prop();
  propsUsed_.colsSet.emplace(colName);
}

void PropertyTrackerVisitor::visit(AttributeExpression *expr) {
  auto *lhs = expr->left();
  auto *rhs = expr->right();
  if (rhs->kind() != Expression::Kind::kConstant) {
    return;
  }
  auto *constExpr = static_cast<ConstantExpression *>(rhs);
  auto &constVal = constExpr->value();
  if (constVal.type() != Value::Type::STRING) {
    return;
  }
  auto &propName = constVal.getStr();
  switch (lhs->kind()) {
    case Expression::Kind::kVarProperty: {  // $e.name
      auto *varPropExpr = static_cast<VariablePropertyExpression *>(lhs);
      auto &edgeAlias = varPropExpr->prop();
      propsUsed_.edgePropsMap[edgeAlias][0].emplace(propName);
      break;
    }
    case Expression::Kind::kInputProperty: {
      auto *inputPropExpr = static_cast<InputPropertyExpression *>(lhs);
      auto &edgeAlias = inputPropExpr->prop();
      propsUsed_.edgePropsMap[edgeAlias][0].emplace(propName);
      break;
    }
    case Expression::Kind::kSubscript: {  // $-.e[0].name
      auto *subscriptExpr = static_cast<SubscriptExpression *>(lhs);
      auto *subLeftExpr = subscriptExpr->left();
      if (subLeftExpr->kind() == Expression::Kind::kInputProperty) {
        auto *inputPropExpr = static_cast<InputPropertyExpression *>(subLeftExpr);
        auto &edgeAlias = inputPropExpr->prop();
        propsUsed_.edgePropsMap[edgeAlias][0].emplace(propName);
      }
    }
    default:
      break;
  }
}

void PropertyTrackerVisitor::visit(FunctionCallExpression *expr) {
  static const std::unordered_set<std::string> kVertexIgnoreFuncs = {"id"};
  static const std::unordered_set<std::string> kEdgeIgnoreFuncs = {
      "src", "dst", "type", "typeid", "rank"};

  auto funName = expr->name();
  if (kVertexIgnoreFuncs.find(funName) != kVertexIgnoreFuncs.end()) {
    DCHECK_EQ(expr->args()->numArgs(), 1);
    auto argExpr = expr->args()->args()[0];
    auto nodeAlias = extractColNameFromInputPropOrVarPropExpr(argExpr);
    if (!nodeAlias.empty()) {
      auto it = propsUsed_.vertexPropsMap.find(nodeAlias);
      if (it == propsUsed_.vertexPropsMap.end()) {
        propsUsed_.vertexPropsMap[nodeAlias] = {};
      }
    }
    return;
  } else if (kEdgeIgnoreFuncs.find(funName) != kEdgeIgnoreFuncs.end()) {
    DCHECK_EQ(expr->args()->numArgs(), 1);
    auto argExpr = expr->args()->args()[0];
    auto edgeAlias = extractColNameFromInputPropOrVarPropExpr(argExpr);
    if (!edgeAlias.empty()) {
      auto it = propsUsed_.edgePropsMap.find(edgeAlias);
      if (it == propsUsed_.edgePropsMap.end()) {
        propsUsed_.edgePropsMap[edgeAlias] = {};
      }
    }
    return;
  }

  for (auto *arg : expr->args()->args()) {
    arg->accept(this);
    if (!ok()) {
      break;
    }
  }
}

void PropertyTrackerVisitor::visit(DestPropertyExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(SourcePropertyExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeSrcIdExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeTypeExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeRankExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeDstIdExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(UUIDExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(VariableExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(VersionedVariableExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(LabelExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(LabelAttributeExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(ConstantExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(ColumnExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(VertexExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeExpression *expr) {
  UNUSED(expr);
}

std::string PropertyTrackerVisitor::extractColNameFromInputPropOrVarPropExpr(
    const Expression *expr) {
  if (expr->kind() == Expression::Kind::kInputProperty) {
    auto *inputPropExpr = static_cast<const InputPropertyExpression *>(expr);
    return inputPropExpr->prop();
  } else if (expr->kind() == Expression::Kind::kVarProperty) {
    auto *varPropExpr = static_cast<const VariablePropertyExpression *>(expr);
    return varPropExpr->prop();
  }
  return "";
}

}  // namespace graph
}  // namespace nebula
