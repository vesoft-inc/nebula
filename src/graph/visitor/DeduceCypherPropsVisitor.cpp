/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/visitor/DeduceCypherPropsVisitor.h"

#include <sstream>

#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

void DeduceCypherPropsVisitor::visit(LabelExpression* expr) {
  auto alias = aliases_.find(expr->name());
  if (alias != aliases_.end()) {
    if (alias->second == AliasType::kNode) {
      cypherProps_.addVertexProp(expr->name());
    } else if (alias->second == AliasType::kEdge) {
      cypherProps_.addEdgeProp(expr->name());
    } else if (alias->second == AliasType::kPath) {
      cypherProps_.addPath(expr->name());
    }
  }
}

void DeduceCypherPropsVisitor::visit(LabelAttributeExpression* expr) {
  auto alias = aliases_.find(expr->left()->name());
  if (alias != aliases_.end()) {
    if (alias->second == AliasType::kNode) {
      cypherProps_.addVertexProp(expr->left()->name(), expr->right()->value().getStr());
    } else if (alias->second == AliasType::kEdge) {
      cypherProps_.addEdgeProp(expr->left()->name(), expr->right()->value().getStr());
    }
  }
}

// optimize the case like `id(v)`
void DeduceCypherPropsVisitor::visit(FunctionCallExpression* expr) {
  if (expr->isFunc("id")) {
    // only get vid so don't need collect properties
    return;
  }
  ExprVisitorImpl::visit(expr);
}

void DeduceCypherPropsVisitor::visit(EdgePropertyExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(TagPropertyExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(InputPropertyExpression* expr) { reportError(expr); }

void DeduceCypherPropsVisitor::visit(VariablePropertyExpression* expr) {
  if (expr->sym()[0] != '_') {
    // user defined variable
    reportError(expr);
  }
}

void DeduceCypherPropsVisitor::visit(SourcePropertyExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(DestPropertyExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(EdgeSrcIdExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(EdgeTypeExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(EdgeRankExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(EdgeDstIdExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(UUIDExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(VariableExpression* expr) {
  if (expr->var()[0] != '_') {
    // user defined variable
    reportError(expr);
  }
}

void DeduceCypherPropsVisitor::visit(VersionedVariableExpression* expr) {
  if (expr->var()[0] != '_') {
    // user defined variable
    reportError(expr);
  }
}

void DeduceCypherPropsVisitor::visit(AttributeExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(ConstantExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(VertexExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(EdgeExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::visit(ColumnExpression* expr) { UNUSED(expr); }

void DeduceCypherPropsVisitor::reportError(const Expression* expr) {
  std::stringstream ss;
  ss << "Not supported expression `" << expr->toString() << "' in cypher.";
  status_ = Status::SemanticError(ss.str());
}

}  // namespace graph
}  // namespace nebula
