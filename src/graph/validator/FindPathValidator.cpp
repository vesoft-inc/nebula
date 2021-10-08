/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/FindPathValidator.h"

#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/util/ValidateUtil.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
  auto fpSentence = static_cast<FindPathSentence*>(sentence_);
  pathCtx_ = getContext<PathContext>();
  pathCtx_->isShortest = fpSentence->isShortest();
  pathCtx_->noLoop = fpSentence->noLoop();
  pathCtx_->withProp = fpSentence->withProp();
  pathCtx_->inputVarName = inputVarName_;

  NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), pathCtx_->from));
  NG_RETURN_IF_ERROR(validateStarts(fpSentence->to(), pathCtx_->to));
  NG_RETURN_IF_ERROR(ValidateUtil::validateOver(qctx_, fpSentence->over(), pathCtx_->over));
  NG_RETURN_IF_ERROR(validateWhere(fpSentence->where()));
  NG_RETURN_IF_ERROR(ValidateUtil::validateStep(fpSentence->step(), pathCtx_->steps));
  NG_RETURN_IF_ERROR(validateYield(fpSentence->yield()));

  return Status::OK();
}

Status FindPathValidator::validateWhere(WhereClause* where) {
  if (where == nullptr) {
    return Status::OK();
  }
  // Not Support $-、$var、$$.tag.prop、$^.tag.prop、agg
  auto expr = where->filter();
  if (ExpressionUtils::findAny(expr,
                               {Expression::Kind::kAggregate,
                                Expression::Kind::kSrcProperty,
                                Expression::Kind::kDstProperty,
                                Expression::Kind::kVarProperty,
                                Expression::Kind::kInputProperty})) {
    return Status::SemanticError("Not support `%s' in where sentence.", expr->toString().c_str());
  }
  where->setFilter(ExpressionUtils::rewriteLabelAttr2EdgeProp(expr));
  auto filter = where->filter();

  auto typeStatus = deduceExprType(filter);
  NG_RETURN_IF_ERROR(typeStatus);
  auto type = typeStatus.value();
  if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
      type != Value::Type::__EMPTY__) {
    std::stringstream ss;
    ss << "`" << filter->toString() << "', expected Boolean, "
       << "but was `" << type << "'";
    return Status::SemanticError(ss.str());
  }

  NG_RETURN_IF_ERROR(deduceProps(filter, pathCtx_->exprProps));
  pathCtx_->filter = filter;
  return Status::OK();
}

Status FindPathValidator::validateYield(YieldClause* yield) {
  auto pool = qctx_->objPool();
  if (yield == nullptr) {
    // TODO: compatible with previous version, this will be deprecated in version 3.0
    auto* yieldColumns = new YieldColumns();
    auto* pathExpr = new YieldColumn(LabelExpression::make(pool, "PATH"), "path");
    yieldColumns->addColumn(pathExpr);
    yield = pool->add(new YieldClause(yieldColumns));
  }

  for (auto& col : yield->columns()) {
    if (col->expr()->kind() != Expression::Kind::kLabel || col->expr()->toString() != "PATH") {
      return Status::SemanticError("illegal yield clauses `%s'. only support yield path",
                                   col->toString().c_str());
    }
    outputs_.emplace_back(col->name(), Value::Type::PATH);
  }
  pathCtx_->colNames = getOutColNames();
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
