/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/OrderByValidator.h"

#include <algorithm>  // for find_if
#include <iterator>   // for distance
#include <memory>     // for unique_ptr
#include <set>        // for set, _Rb_tree_cons...

#include "common/base/StatusOr.h"                  // for StatusOr
#include "common/expression/Expression.h"          // for Expression, Expres...
#include "common/expression/PropertyExpression.h"  // for VariablePropertyEx...
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/Symbols.h"                 // for ColDef, ColsDef
#include "graph/context/ValidateContext.h"         // for ValidateContext
#include "graph/planner/plan/ExecutionPlan.h"      // for ExecutionPlan
#include "graph/planner/plan/Query.h"              // for Sort
#include "graph/visitor/DeducePropsVisitor.h"      // for ExpressionProps
#include "parser/TraverseSentences.h"              // for OrderFactor::Order...

namespace nebula {
namespace graph {
Status OrderByValidator::validateImpl() {
  auto sentence = static_cast<OrderBySentence *>(sentence_);
  auto &factors = sentence->factors();
  for (auto &factor : factors) {
    if (factor->expr()->kind() == Expression::Kind::kInputProperty) {
      auto expr = static_cast<InputPropertyExpression *>(factor->expr());
      NG_RETURN_IF_ERROR(deduceExprType(expr));
      NG_RETURN_IF_ERROR(deduceProps(expr, exprProps_));
      const auto &cols = inputCols();
      auto &name = expr->prop();
      auto eq = [&](const ColDef &col) { return col.name == name; };
      auto iter = std::find_if(cols.cbegin(), cols.cend(), eq);
      size_t colIdx = std::distance(cols.cbegin(), iter);
      colOrderTypes_.emplace_back(std::make_pair(colIdx, factor->orderType()));
    } else if (factor->expr()->kind() == Expression::Kind::kVarProperty) {
      auto expr = static_cast<VariablePropertyExpression *>(factor->expr());
      NG_RETURN_IF_ERROR(deduceExprType(expr));
      NG_RETURN_IF_ERROR(deduceProps(expr, exprProps_));
      const auto &cols = vctx_->getVar(expr->sym());
      auto &name = expr->prop();
      auto eq = [&](const ColDef &col) { return col.name == name; };
      auto iter = std::find_if(cols.cbegin(), cols.cend(), eq);
      size_t colIdx = std::distance(cols.cbegin(), iter);
      colOrderTypes_.emplace_back(std::make_pair(colIdx, factor->orderType()));
    } else {
      return Status::SemanticError("Order by with invalid expression `%s'",
                                   factor->expr()->toString().c_str());
    }
  }

  if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
    return Status::SemanticError("Not support both input and variable.");
  } else if (!exprProps_.inputProps().empty()) {
    outputs_ = inputCols();
  } else if (!exprProps_.varProps().empty()) {
    if (!userDefinedVarNameList_.empty()) {
      if (userDefinedVarNameList_.size() != 1) {
        return Status::SemanticError("Multiple user defined vars are not supported yet.");
      }
      userDefinedVarName_ = *userDefinedVarNameList_.begin();
      outputs_ = vctx_->getVar(userDefinedVarName_);
    }
  }

  return Status::OK();
}

Status OrderByValidator::toPlan() {
  auto *plan = qctx_->plan();
  auto *sortNode = Sort::make(qctx_, plan->root(), std::move(colOrderTypes_));
  std::vector<std::string> colNames;
  for (auto &col : outputs_) {
    colNames.emplace_back(col.name);
  }
  sortNode->setColNames(std::move(colNames));
  if (!userDefinedVarName_.empty()) {
    sortNode->setInputVar(userDefinedVarName_);
  }

  root_ = sortNode;
  tail_ = root_;
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
