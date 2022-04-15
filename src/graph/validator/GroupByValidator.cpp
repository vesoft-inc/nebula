/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/GroupByValidator.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/AnonColGenerator.h"
#include "graph/util/AnonVarGenerator.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/FindVisitor.h"

namespace nebula {
namespace graph {

Status GroupByValidator::validateImpl() {
  auto* groupBySentence = static_cast<GroupBySentence*>(sentence_);
  NG_RETURN_IF_ERROR(validateGroup(groupBySentence->groupClause()));
  NG_RETURN_IF_ERROR(validateYield(groupBySentence->yieldClause()));
  NG_RETURN_IF_ERROR(groupClauseSemanticCheck());

  for (auto* col : groupBySentence->yieldClause()->columns()) {
    auto type = deduceExprType(col->expr());
    outputs_.emplace_back(col->name(), std::move(type).value());
  }

  return Status::OK();
}

// Validate the yield clause of group by.
// Check aggregate expression, and add Input/Variable expression for Project
Status GroupByValidator::validateYield(const YieldClause* yieldClause) {
  std::vector<YieldColumn*> columns;
  if (yieldClause != nullptr) {
    columns = yieldClause->columns();
  }
  if (columns.empty()) {
    return Status::SemanticError("Yield cols is Empty");
  }

  auto* pool = qctx_->objPool();
  projCols_ = pool->makeAndAdd<YieldColumns>();
  for (auto* col : columns) {
    auto colOldName = col->name();
    auto* colExpr = col->expr();
    if (col->expr()->kind() != Expression::Kind::kAggregate) {
      auto collectAggCol = colExpr->clone();
      auto aggs = ExpressionUtils::collectAll(collectAggCol, {Expression::Kind::kAggregate});
      for (auto* agg : aggs) {
        DCHECK_EQ(agg->kind(), Expression::Kind::kAggregate);
        NG_RETURN_IF_ERROR(
            ExpressionUtils::checkAggExpr(static_cast<const AggregateExpression*>(agg)));
        aggOutputColNames_.emplace_back(agg->toString());
        groupItems_.emplace_back(agg->clone());
        needGenProject_ = true;
      }
      if (!aggs.empty()) {
        auto* colRewrited = ExpressionUtils::rewriteAgg2VarProp(colExpr->clone());
        projCols_->addColumn(new YieldColumn(colRewrited, colOldName));
        continue;
      }
    }

    // collect exprs for check later
    if (colExpr->kind() == Expression::Kind::kAggregate) {
      auto* aggExpr = static_cast<AggregateExpression*>(colExpr);
      NG_RETURN_IF_ERROR(ExpressionUtils::checkAggExpr(aggExpr));
    } else if (!ExpressionUtils::isEvaluableExpr(colExpr, qctx_)) {
      yieldCols_.emplace_back(colExpr);
    }

    groupItems_.emplace_back(colExpr);
    aggOutputColNames_.emplace_back(colOldName);

    projCols_->addColumn(
        new YieldColumn(VariablePropertyExpression::make(pool, "", colOldName), colOldName));

    ExpressionProps yieldProps;
    NG_RETURN_IF_ERROR(deduceProps(colExpr, yieldProps));
    // TODO: refactor exprProps_ related logic
    exprProps_.unionProps(std::move(yieldProps));
  }

  return Status::OK();
}

// Validate group by expression.
// The expression must contains Input/Variable expression, and shouldn't contains some expressions.
Status GroupByValidator::validateGroup(const GroupClause* groupClause) {
  if (!groupClause) return Status::OK();
  std::vector<YieldColumn*> columns;
  if (groupClause != nullptr) {
    columns = groupClause->columns();
  }

  auto groupByValid = [](Expression::Kind kind) -> bool {
    return std::unordered_set<Expression::Kind>{Expression::Kind::kAdd,
                                                Expression::Kind::kMinus,
                                                Expression::Kind::kMultiply,
                                                Expression::Kind::kDivision,
                                                Expression::Kind::kMod,
                                                Expression::Kind::kTypeCasting,
                                                Expression::Kind::kFunctionCall,
                                                Expression::Kind::kInputProperty,
                                                Expression::Kind::kVarProperty,
                                                Expression::Kind::kCase}
        .count(kind);
  };
  for (auto* col : columns) {
    if (graph::ExpressionUtils::findAny(col->expr(), {Expression::Kind::kAggregate}) ||
        !graph::ExpressionUtils::findAny(
            col->expr(), {Expression::Kind::kInputProperty, Expression::Kind::kVarProperty})) {
      return Status::SemanticError("Group `%s' invalid", col->expr()->toString().c_str());
    }
    if (!groupByValid(col->expr()->kind())) {
      return Status::SemanticError("Group `%s' invalid", col->expr()->toString().c_str());
    }

    NG_RETURN_IF_ERROR(deduceExprType(col->expr()));
    NG_RETURN_IF_ERROR(deduceProps(col->expr(), exprProps_));

    groupKeys_.emplace_back(col->expr());
  }
  return Status::OK();
}

Status GroupByValidator::toPlan() {
  auto* groupBy = Aggregate::make(qctx_, nullptr, std::move(groupKeys_), std::move(groupItems_));
  groupBy->setColNames(aggOutputColNames_);
  if (!exprProps_.varProps().empty() && !userDefinedVarNameList_.empty()) {
    if (userDefinedVarNameList_.size() != 1) {
      return Status::SemanticError("Multiple user defined vars not supported yet.");
    }
    auto userDefinedVar = *userDefinedVarNameList_.begin();
    if (!userDefinedVar.empty()) {
      groupBy->setInputVar(userDefinedVar);
    } else {
      return Status::SemanticError("Invalid anonymous user defined var.");
    }
  }

  if (needGenProject_) {
    // rewrite Expr which has inner aggExpr and push it up to Project.
    root_ = Project::make(qctx_, groupBy, projCols_);
  } else {
    root_ = groupBy;
  }
  tail_ = groupBy;
  return Status::OK();
}

// Check the correctness of group by clause and yield clause.
// Check expression type,  disable invalid properties.
// Check group by expression and yield expression, variable expression in yield columns must
// contains in group by expressions too.
Status GroupByValidator::groupClauseSemanticCheck() {
  // deduce group items and build outputs_
  DCHECK_EQ(aggOutputColNames_.size(), groupItems_.size());
  for (auto i = 0u; i < groupItems_.size(); ++i) {
    auto type = deduceExprType(groupItems_[i]);
    NG_RETURN_IF_ERROR(type);
  }
  // check exprProps
  if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
    return Status::SemanticError("Only support input and variable in GroupBy sentence.");
  }
  if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
    return Status::SemanticError("Not support both input and variable in GroupBy sentence.");
  }
  if (!exprProps_.varProps().empty() && exprProps_.varProps().size() > 1) {
    return Status::SemanticError("Only one variable allowed to use.");
  }

  if (groupKeys_.empty()) {
    groupKeys_ = yieldCols_;
  } else {
    std::unordered_set<Expression*> groupSet(groupKeys_.begin(), groupKeys_.end());
    auto finder = [&groupSet](const Expression* expr) -> bool {
      for (auto* target : groupSet) {
        if (*target == *expr) {
          return true;
        }
      }
      return false;
    };
    for (auto* expr : yieldCols_) {
      if (ExpressionUtils::isEvaluableExpr(expr, qctx_)) {
        continue;
      }
      FindVisitor visitor(finder);
      expr->accept(&visitor);
      if (!visitor.found()) {
        return Status::SemanticError(
            "Yield non-agg expression `%s' must be"
            " functionally dependent on items in GROUP BY clause",
            expr->toString().c_str());
      }
    }
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
