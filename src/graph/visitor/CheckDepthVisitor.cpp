/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/CheckDepthVisitor.h"

#include <sstream>
#include <unordered_map>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/function/FunctionManager.h"
#include "graph/context/QueryContext.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/context/ValidateContext.h"
#include "graph/util/SchemaUtil.h"
#include "graph/visitor/EvaluableExprVisitor.h"

namespace nebula {
namespace graph {

CheckDepthVisitor::CheckDepthVisitor() {}

void CheckDepthVisitor::visit(ConstantExpression *) {}

void CheckDepthVisitor::visit(UnaryExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->operand()->accept(this);
}

void CheckDepthVisitor::visit(TypeCastingExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->operand()->accept(this);
  if (!ok()) return;
}

void CheckDepthVisitor::visit(LabelExpression *) {}

void CheckDepthVisitor::visit(ArithmeticExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->left()->accept(this);
  if (!ok()) return;
  expr->right()->accept(this);
}

void CheckDepthVisitor::visit(RelationalExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->left()->accept(this);
  if (!ok()) return;
  expr->right()->accept(this);
}

void CheckDepthVisitor::visit(SubscriptExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->left()->accept(this);
  if (!ok()) return;
  expr->right()->accept(this);
}

void CheckDepthVisitor::visit(AttributeExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->left()->accept(this);
  if (!ok()) return;
  expr->right()->accept(this);
}

void CheckDepthVisitor::visit(LogicalExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  auto &operands = expr->operands();
  for (auto i = 0u; i < operands.size(); i++) {
    operands[i]->accept(this);
    if (!ok()) return;
  }
}

void CheckDepthVisitor::visit(LabelAttributeExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  const_cast<LabelExpression *>(expr->left())->accept(this);
  if (!ok()) return;
  const_cast<ConstantExpression *>(expr->right())->accept(this);
}

void CheckDepthVisitor::visit(FunctionCallExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  for (auto &arg : expr->args()->args()) {
    arg->accept(this);
    if (!ok()) return;
  }
}

void CheckDepthVisitor::visit(AggregateExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->arg()->accept(this);
}

void CheckDepthVisitor::visit(CaseExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  if (expr->hasCondition()) {
    expr->condition()->accept(this);
    if (!ok()) return;
  }
  if (expr->hasDefault()) {
    expr->defaultResult()->accept(this);
    if (!ok()) return;
  }
  for (const auto &whenThen : expr->cases()) {
    whenThen.when->accept(this);
    if (!ok()) return;
    whenThen.then->accept(this);
    if (!ok()) return;
  }
}

void CheckDepthVisitor::visit(PredicateExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  if (expr->hasFilter()) {
    expr->filter()->accept(this);
    if (!ok()) {
      return;
    }
  }
  expr->collection()->accept(this);
}

void CheckDepthVisitor::visit(ListComprehensionExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  if (expr->hasFilter()) {
    expr->filter()->accept(this);
    if (!ok()) {
      return;
    }
  }
  if (expr->hasMapping()) {
    expr->mapping()->accept(this);
    if (!ok()) {
      return;
    }
  }
  expr->collection()->accept(this);
  if (!ok()) {
    return;
  }
}

void CheckDepthVisitor::visit(ReduceExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->initial()->accept(this);
  if (!ok()) return;
  expr->mapping()->accept(this);
  if (!ok()) return;
  expr->collection()->accept(this);
  if (!ok()) return;
}

void CheckDepthVisitor::visit(SubscriptRangeExpression *expr) {
  checkDepth();
  SCOPE_EXIT { recoverDepth(); };
  if (!ok()) return;
  expr->list()->accept(this);
  if (!ok()) {
    return;
  }
  if (expr->lo() != nullptr) {
    expr->lo()->accept(this);
    if (!ok()) {
      return;
    }
  }

  if (expr->hi() != nullptr) {
    expr->hi()->accept(this);
    if (!ok()) {
      return;
    }
  }
}

void CheckDepthVisitor::visit(UUIDExpression *) {}

void CheckDepthVisitor::visit(VariableExpression *) {}

void CheckDepthVisitor::visit(VersionedVariableExpression *) {}

void CheckDepthVisitor::visit(ListExpression *) {}

void CheckDepthVisitor::visit(SetExpression *) {}

void CheckDepthVisitor::visit(MapExpression *) {}

void CheckDepthVisitor::visit(TagPropertyExpression *) {}

void CheckDepthVisitor::visit(EdgePropertyExpression *) {}

void CheckDepthVisitor::visit(InputPropertyExpression *) {}

void CheckDepthVisitor::visit(VariablePropertyExpression *) {}

void CheckDepthVisitor::visit(DestPropertyExpression *) {}

void CheckDepthVisitor::visit(SourcePropertyExpression *) {}

void CheckDepthVisitor::visit(EdgeSrcIdExpression *) {}

void CheckDepthVisitor::visit(EdgeTypeExpression *) {}

void CheckDepthVisitor::visit(EdgeRankExpression *) {}

void CheckDepthVisitor::visit(EdgeDstIdExpression *) {}

void CheckDepthVisitor::visit(VertexExpression *) {}

void CheckDepthVisitor::visit(EdgeExpression *) {}

void CheckDepthVisitor::visit(ColumnExpression *) {}

void CheckDepthVisitor::visit(PathBuildExpression *) {}
}  // namespace graph
}  // namespace nebula
