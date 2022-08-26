/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/MatchSolver.h"

#include "common/expression/UnaryExpression.h"
#include "graph/context/ast/AstContext.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
Expression* MatchSolver::rewriteLabel2Vertex(QueryContext* qctx, const Expression* expr) {
  auto* pool = qctx->objPool();
  auto matcher = [](const Expression* e) -> bool {
    return e->kind() == Expression::Kind::kLabel || e->kind() == Expression::Kind::kLabelAttribute;
  };
  auto rewriter = [&, pool](const Expression* e) -> Expression* {
    DCHECK(e->kind() == Expression::Kind::kLabelAttribute || e->kind() == Expression::Kind::kLabel);
    if (e->kind() == Expression::Kind::kLabelAttribute) {
      auto la = static_cast<const LabelAttributeExpression*>(e);
      return AttributeExpression::make(pool, VertexExpression::make(pool), la->right()->clone());
    }
    return VertexExpression::make(pool);
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* MatchSolver::rewriteLabel2Edge(QueryContext* qctx, const Expression* expr) {
  auto* pool = qctx->objPool();
  auto matcher = [](const Expression* e) -> bool {
    return e->kind() == Expression::Kind::kLabel || e->kind() == Expression::Kind::kLabelAttribute;
  };
  auto rewriter = [&pool](const Expression* e) -> Expression* {
    DCHECK(e->kind() == Expression::Kind::kLabelAttribute || e->kind() == Expression::Kind::kLabel);
    if (e->kind() == Expression::Kind::kLabelAttribute) {
      auto la = static_cast<const LabelAttributeExpression*>(e);
      return AttributeExpression::make(pool, EdgeExpression::make(pool), la->right()->clone());
    }
    return EdgeExpression::make(pool);
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* MatchSolver::rewriteLabel2VarProp(QueryContext* qctx, const Expression* expr) {
  auto* pool = qctx->objPool();
  auto matcher = [](const Expression* e) -> bool {
    return e->kind() == Expression::Kind::kLabel || e->kind() == Expression::Kind::kLabelAttribute;
  };
  auto rewriter = [&pool](const Expression* e) -> Expression* {
    DCHECK(e->kind() == Expression::Kind::kLabelAttribute || e->kind() == Expression::Kind::kLabel);
    if (e->kind() == Expression::Kind::kLabelAttribute) {
      auto* la = static_cast<const LabelAttributeExpression*>(e);
      auto* var = VariablePropertyExpression::make(pool, "", la->left()->name());
      return AttributeExpression::make(
          pool, var, ConstantExpression::make(pool, la->right()->value()));
    }
    auto label = static_cast<const LabelExpression*>(e);
    return VariablePropertyExpression::make(pool, "", label->name());
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* MatchSolver::doRewrite(QueryContext* qctx,
                                   const std::unordered_map<std::string, AliasType>& aliases,
                                   const Expression* expr) {
  if (expr->kind() == Expression::Kind::kLabel) {
    auto* labelExpr = static_cast<const LabelExpression*>(expr);
    auto alias = aliases.find(labelExpr->name());
    DCHECK(alias != aliases.end());
  }
  return rewriteLabel2VarProp(qctx, expr);
}

Expression* MatchSolver::makeIndexFilter(const std::string& label,
                                         const MapExpression* map,
                                         QueryContext* qctx,
                                         bool isEdgeProperties) {
  auto makePropExpr = [=, &label](const std::string& prop) -> Expression* {
    if (isEdgeProperties) {
      return EdgePropertyExpression::make(qctx->objPool(), label, prop);
    }
    return TagPropertyExpression::make(qctx->objPool(), label, prop);
  };

  std::vector<Expression*> operands;
  operands.reserve(map->size());
  for (const auto& item : map->items()) {
    operands.emplace_back(RelationalExpression::makeEQ(
        qctx->objPool(), makePropExpr(item.first), item.second->clone()));
  }
  Expression* root = nullptr;
  DCHECK(!operands.empty());
  if (operands.size() == 1) {
    root = operands.front();
  } else {
    root = LogicalExpression::makeAnd(qctx->objPool());
    static_cast<LogicalExpression*>(root)->setOperands(std::move(operands));
  }
  return root;
}

Expression* MatchSolver::makeIndexFilter(const std::string& label,
                                         const std::string& alias,
                                         Expression* filter,
                                         QueryContext* qctx,
                                         bool isEdgeProperties) {
  static const std::unordered_set<Expression::Kind> kinds = {
      Expression::Kind::kRelEQ,
      Expression::Kind::kRelLT,
      Expression::Kind::kRelLE,
      Expression::Kind::kRelGT,
      Expression::Kind::kRelGE,
  };

  std::vector<const Expression*> ands;
  auto kind = filter->kind();
  if (kinds.count(kind) == 1) {
    ands.emplace_back(filter);
  } else if (kind == Expression::Kind::kLogicalAnd) {
    auto* logic = static_cast<LogicalExpression*>(filter);
    ExpressionUtils::pullAnds(logic);
    for (auto& operand : logic->operands()) {
      ands.emplace_back(operand);
    }
  } else {
    return nullptr;
  }

  auto* pool = qctx->objPool();
  std::vector<Expression*> relationals;
  for (auto* item : ands) {
    if (kinds.count(item->kind()) != 1) {
      continue;
    }

    auto* binary = static_cast<const BinaryExpression*>(item);
    auto* left = binary->left();
    auto* right = binary->right();
    const ConstantExpression* constant = nullptr;
    std::string propName;
    // TODO(aiee) extract the logic that applies to both match and lookup
    if (isEdgeProperties) {
      const LabelAttributeExpression* la = nullptr;
      if (left->kind() == Expression::Kind::kLabelAttribute &&
          right->kind() == Expression::Kind::kConstant) {
        la = static_cast<const LabelAttributeExpression*>(left);
        constant = static_cast<const ConstantExpression*>(right);
      } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                 left->kind() == Expression::Kind::kConstant) {
        la = static_cast<const LabelAttributeExpression*>(right);
        constant = static_cast<const ConstantExpression*>(left);
      } else {
        continue;
      }
      if (la->left()->name() != alias) {
        continue;
      }
      propName = la->right()->value().getStr();
    } else {
      const LabelTagPropertyExpression* la = nullptr;
      if (left->kind() == Expression::Kind::kLabelTagProperty &&
          right->kind() == Expression::Kind::kConstant) {
        la = static_cast<const LabelTagPropertyExpression*>(left);
        constant = static_cast<const ConstantExpression*>(right);
      } else if (right->kind() == Expression::Kind::kLabelTagProperty &&
                 left->kind() == Expression::Kind::kConstant) {
        la = static_cast<const LabelTagPropertyExpression*>(right);
        constant = static_cast<const ConstantExpression*>(left);
      } else {
        continue;
      }
      if (static_cast<const PropertyExpression*>(la->label())->prop() != alias ||
          la->sym() != label) {
        continue;
      }
      propName = la->prop();
    }

    auto* tpExpr =
        isEdgeProperties
            ? static_cast<Expression*>(EdgePropertyExpression::make(pool, label, propName))
            : static_cast<Expression*>(TagPropertyExpression::make(pool, label, propName));
    auto* newConstant = constant->clone();
    if (right->kind() == Expression::Kind::kConstant) {
      auto* rel = RelationalExpression::makeKind(pool, item->kind(), tpExpr, newConstant);
      relationals.emplace_back(rel);
    } else {
      auto* rel = RelationalExpression::makeKind(pool, item->kind(), newConstant, tpExpr);
      relationals.emplace_back(rel);
    }
  }

  if (relationals.empty()) {
    return nullptr;
  }

  auto* root = relationals[0];
  for (auto i = 1u; i < relationals.size(); i++) {
    auto* left = root;
    root = LogicalExpression::makeAnd(qctx->objPool(), left, relationals[i]);
  }

  return root;
}

PlanNode* MatchSolver::filtPathHasSameEdge(PlanNode* input,
                                           const std::string& column,
                                           QueryContext* qctx) {
  auto* pool = qctx->objPool();
  auto args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, column));
  auto fnCall = FunctionCallExpression::make(pool, "hasSameEdgeInPath", args);
  auto cond = UnaryExpression::makeNot(pool, fnCall);
  auto filter = Filter::make(qctx, input, cond);
  filter->setColNames(input->colNames());
  return filter;
}

static YieldColumn* buildVertexColumn(ObjectPool* pool, const std::string& alias) {
  return new YieldColumn(InputPropertyExpression::make(pool, alias), alias);
}

static YieldColumn* buildEdgeColumn(ObjectPool* pool, const EdgeInfo& edge) {
  Expression* expr = nullptr;
  if (edge.range == nullptr) {
    expr = SubscriptExpression::make(
        pool, InputPropertyExpression::make(pool, edge.alias), ConstantExpression::make(pool, 0));
  } else {
    auto* args = ArgumentList::make(pool);
    args->addArgument(VariableExpression::make(pool, "e"));
    auto* filter = FunctionCallExpression::make(pool, "is_edge", args);
    expr = ListComprehensionExpression::make(
        pool, "e", InputPropertyExpression::make(pool, edge.alias), filter);
  }
  return new YieldColumn(expr, edge.alias);
}

// static
void MatchSolver::buildProjectColumns(QueryContext* qctx, Path& path, SubPlan& plan) {
  auto columns = qctx->objPool()->makeAndAdd<YieldColumns>();
  std::vector<std::string> colNames;
  auto& nodeInfos = path.nodeInfos;
  auto& edgeInfos = path.edgeInfos;

  auto addNode = [columns, &colNames, qctx](auto& nodeInfo) {
    if (!nodeInfo.alias.empty() && !nodeInfo.anonymous) {
      columns->addColumn(buildVertexColumn(qctx->objPool(), nodeInfo.alias));
      colNames.emplace_back(nodeInfo.alias);
    }
  };

  auto addEdge = [columns, &colNames, qctx](auto& edgeInfo) {
    if (!edgeInfo.alias.empty() && !edgeInfo.anonymous) {
      columns->addColumn(buildEdgeColumn(qctx->objPool(), edgeInfo));
      colNames.emplace_back(edgeInfo.alias);
    }
  };

  for (size_t i = 0; i < edgeInfos.size(); i++) {
    addNode(nodeInfos[i]);
    addEdge(edgeInfos[i]);
  }

  // last vertex
  DCHECK(!nodeInfos.empty());
  addNode(nodeInfos.back());

  if (!path.anonymous) {
    DCHECK(!path.alias.empty());
    columns->addColumn(new YieldColumn(DCHECK_NOTNULL(path.pathBuild), path.alias));
    colNames.emplace_back(path.alias);
  }

  auto project = Project::make(qctx, plan.root, columns);
  project->setColNames(std::move(colNames));

  plan.root = project;
}

}  // namespace graph
}  // namespace nebula
