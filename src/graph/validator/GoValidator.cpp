/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/GoValidator.h"

#include "graph/planner/plan/Logic.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/ValidateUtil.h"
#include "graph/visitor/ExtractPropExprVisitor.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

Status GoValidator::validateImpl() {
  auto* goSentence = static_cast<GoSentence*>(sentence_);
  goCtx_ = getContext<GoContext>();
  goCtx_->inputVarName = inputVarName_;

  NG_RETURN_IF_ERROR(ValidateUtil::validateStep(goSentence->stepClause(), goCtx_->steps));
  NG_RETURN_IF_ERROR(validateStarts(goSentence->fromClause(), goCtx_->from));
  NG_RETURN_IF_ERROR(ValidateUtil::validateOver(qctx_, goSentence->overClause(), goCtx_->over));
  NG_RETURN_IF_ERROR(extractTagIds());
  NG_RETURN_IF_ERROR(validateWhere(goSentence->whereClause()));
  NG_RETURN_IF_ERROR(validateYield(goSentence->yieldClause()));
  NG_RETURN_IF_ERROR(validateTruncate(goSentence->truncateClause()));

  const auto& exprProps = goCtx_->exprProps;
  if (!exprProps.inputProps().empty() && goCtx_->from.fromType != kPipe) {
    return Status::SemanticError("$- must be referred in FROM before used in WHERE or YIELD");
  }

  // Only one variable allowed in whole sentence
  if (!exprProps.varProps().empty()) {
    if (goCtx_->from.fromType != kVariable) {
      return Status::SemanticError(
          "A variable must be referred in FROM before used in WHERE or YIELD");
    }
    auto varPropsMap = exprProps.varProps();
    std::vector<std::string> keys;
    for (const auto& elem : varPropsMap) {
      keys.emplace_back(elem.first);
    }
    if (keys.size() > 1) {
      return Status::SemanticError("Multiple variable property is not supported in WHERE or YIELD");
    }
    if (keys.front() != goCtx_->from.userDefinedVarName) {
      return Status::SemanticError(
          "A variable must be referred in FROM before used in WHERE or YIELD");
    }
  }

  if ((!exprProps.inputProps().empty() && !exprProps.varProps().empty()) ||
      exprProps.varProps().size() > 1) {
    return Status::SemanticError("Only support single input in a go sentence.");
  }

  goCtx_->isSimple = isSimpleCase();
  if (goCtx_->isSimple) {
    // Need to unify all EdgeDstIdExpr to *._dst.
    // eg. serve._dst will be unified to *._dst
    rewrite2EdgeDst();
  }
  NG_RETURN_IF_ERROR(buildColumns());
  if (goCtx_->isSimple) {
    auto iter = propExprColMap_.find("*._dst");
    if (iter != propExprColMap_.end()) {
      goCtx_->dstIdColName = iter->second->alias();
    }
    // Rewrite *._dst/serve._dst to $dstIdColName
    rewriteEdgeDst2VarProp();
  }
  return Status::OK();
}

// Validate filter expression, rewrites expression to fit sementic,
// deduce and check the type of expression, collect properties used in filter.
Status GoValidator::validateWhere(WhereClause* where) {
  if (where == nullptr) {
    return Status::OK();
  }

  auto expr = where->filter();
  where->setFilter(ExpressionUtils::rewriteLabelAttr2EdgeProp(expr));
  auto foldRes = ExpressionUtils::foldConstantExpr(where->filter());
  NG_RETURN_IF_ERROR(foldRes);

  auto filter = foldRes.value();
  auto typeStatus = deduceExprType(filter);
  NG_RETURN_IF_ERROR(typeStatus);
  auto type = typeStatus.value();
  if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
      type != Value::Type::__EMPTY__) {
    std::stringstream ss;
    ss << "`" << filter->toString() << "', expected boolean, "
       << "but was `" << type << "'";
    return Status::SemanticError(ss.str());
  }

  NG_RETURN_IF_ERROR(deduceProps(filter, goCtx_->exprProps, &tagIds_, &goCtx_->over.edgeTypes));
  goCtx_->filter = filter;
  return Status::OK();
}

// Validate the step sample/limit clause, which specify the sample/limit number for each steps.
Status GoValidator::validateTruncate(TruncateClause* truncate) {
  if (truncate == nullptr) {
    return Status::OK();
  }
  auto* tExpr = truncate->truncate();
  goCtx_->random = truncate->isSample();
  auto qeCtx = QueryExpressionContext();
  auto limits = tExpr->eval(qeCtx);
  if (!limits.isList()) {
    return Status::SemanticError("`%s' type must be LIST.", tExpr->toString().c_str());
  }
  // length of list must be equal to N step
  const auto& steps = goCtx_->steps;
  uint32_t totalSteps = steps.isMToN() ? steps.nSteps() : steps.steps();
  if (limits.getList().size() != totalSteps) {
    return Status::SemanticError(
        "`%s' length must be equal to GO step size %d.", tExpr->toString().c_str(), totalSteps);
  }
  goCtx_->limits.reserve(limits.getList().size());
  for (auto& ele : limits.getList().values) {
    // check if element of list is nonnegative integer
    if (!ele.isInt()) {
      return Status::SemanticError("Limit/Sample element type must be Integer.");
    }
    if (ele.getInt() < 0) {
      return Status::SemanticError("Limit/Sample element must be nonnegative.");
    }
    goCtx_->limits.emplace_back(ele.getInt());
  }
  return Status::OK();
}

// Validate yield clause, disable the invalid expression types, rewrites expression to fit sementic,
// check expression type, collect properties used in yield.
Status GoValidator::validateYield(YieldClause* yield) {
  if (yield == nullptr) {
    return Status::SemanticError("Missing yield clause.");
  }
  goCtx_->distinct = yield->isDistinct();
  auto& exprProps = goCtx_->exprProps;

  for (auto col : yield->columns()) {
    const auto& colName = col->name();
    auto vertexExpr = ExpressionUtils::findAny(col->expr(), {Expression::Kind::kVertex});
    if (vertexExpr != nullptr &&
        static_cast<const VertexExpression*>(vertexExpr)->name() == "VERTEX") {
      return Status::SemanticError("`%s' is not support in go sentence.", col->toString().c_str());
    }

    col->setExpr(rewriteVertexEdge2EdgeProp(col->expr()));
    col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));
    NG_RETURN_IF_ERROR(ValidateUtil::invalidLabelIdentifiers(col->expr()));

    auto* colExpr = col->expr();
    auto typeStatus = deduceExprType(colExpr);
    NG_RETURN_IF_ERROR(typeStatus);
    auto type = typeStatus.value();
    outputs_.emplace_back(colName, type);
    NG_RETURN_IF_ERROR(deduceProps(colExpr, exprProps, &tagIds_, &goCtx_->over.edgeTypes));
  }

  const auto& over = goCtx_->over;
  for (const auto& e : exprProps.edgeProps()) {
    auto found = std::find(over.edgeTypes.begin(), over.edgeTypes.end(), e.first);
    if (found == over.edgeTypes.end()) {
      return Status::SemanticError("edge should be declared first in over clause.");
    }
  }
  goCtx_->yieldExpr = yield->yields();
  goCtx_->colNames = getOutColNames();
  return Status::OK();
}

// Get all tag IDs in the whole space
Status GoValidator::extractTagIds() {
  auto tagStatus = qctx_->schemaMng()->getAllLatestVerTagSchema(space_.id);
  NG_RETURN_IF_ERROR(tagStatus);
  for (const auto& tag : tagStatus.value()) {
    tagIds_.emplace_back(tag.first);
  }
  return Status::OK();
}

Status GoValidator::extractPropExprs(const Expression* expr,
                                     std::unordered_set<std::string>& uniqueExpr) {
  ExtractPropExprVisitor visitor(vctx_,
                                 goCtx_->srcPropsExpr,
                                 goCtx_->edgePropsExpr,
                                 goCtx_->dstPropsExpr,
                                 inputPropCols_,
                                 propExprColMap_,
                                 uniqueExpr);
  const_cast<Expression*>(expr)->accept(&visitor);
  return std::move(visitor).status();
}

Expression* GoValidator::rewriteVertexEdge2EdgeProp(const Expression* expr) {
  auto pool = qctx_->objPool();
  auto matcher = [](const Expression* e) -> bool {
    if (e->kind() != Expression::Kind::kFunctionCall) {
      return false;
    }
    auto name = e->toString();
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    std::unordered_set<std::string> colNames({"id($$)", "id($^)", "rank(edge)", "typeid(edge)"});
    if (colNames.find(name) != colNames.end()) {
      return true;
    }
    return false;
  };

  auto rewriter = [&, pool](const Expression* e) -> Expression* {
    DCHECK(e->kind() == Expression::Kind::kFunctionCall);
    auto name = e->toString();
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    if (name == "id($$)") {
      return EdgeDstIdExpression::make(pool, "*");
    } else if (name == "id($^)") {
      return EdgeSrcIdExpression::make(pool, "*");
    } else if (name == "rank(edge)") {
      return EdgeRankExpression::make(pool, "*");
    } else {
      return EdgeTypeExpression::make(pool, "*");
    }
  };
  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// Rewrites the property expression to corresponding Variable/Input expression
// which get related property from previous plan node.
Expression* GoValidator::rewrite2VarProp(const Expression* expr) {
  auto matcher = [this](const Expression* e) -> bool {
    return propExprColMap_.find(e->toString()) != propExprColMap_.end();
  };
  auto rewriter = [this](const Expression* e) -> Expression* {
    auto iter = propExprColMap_.find(e->toString());
    DCHECK(iter != propExprColMap_.end());
    return VariablePropertyExpression::make(qctx_->objPool(), "", iter->second->alias());
  };

  return RewriteVisitor::transform(expr, matcher, rewriter);
}

// Build the final output columns, collect the src/edge and dst properties used in the query,
// collect the input properties used in the query,
// rewrites output expression to Input/Variable expression to get properties from previous plan node
Status GoValidator::buildColumns() {
  const auto& exprProps = goCtx_->exprProps;
  const auto& dstTagProps = exprProps.dstTagProps();
  const auto& inputProps = exprProps.inputProps();
  const auto& varProps = exprProps.varProps();
  const auto& from = goCtx_->from;
  auto pool = qctx_->objPool();

  if (!exprProps.srcTagProps().empty()) {
    goCtx_->srcPropsExpr = pool->makeAndAdd<YieldColumns>();
  }
  if (!exprProps.edgeProps().empty()) {
    goCtx_->edgePropsExpr = pool->makeAndAdd<YieldColumns>();
  }

  if (dstTagProps.empty() && inputProps.empty() && varProps.empty() &&
      from.fromType == FromType::kInstantExpr) {
    return Status::OK();
  }

  if (!dstTagProps.empty()) {
    goCtx_->dstPropsExpr = pool->makeAndAdd<YieldColumns>();
  }

  if (!inputProps.empty() || !varProps.empty()) {
    inputPropCols_ = pool->makeAndAdd<YieldColumns>();
  }

  std::unordered_set<std::string> uniqueEdgeVertexExpr;
  auto filter = goCtx_->filter;
  if (filter != nullptr) {
    NG_RETURN_IF_ERROR(extractPropExprs(filter, uniqueEdgeVertexExpr));
    goCtx_->filter = rewrite2VarProp(filter);
  }

  auto* newYieldExpr = pool->makeAndAdd<YieldColumns>();
  for (auto* col : goCtx_->yieldExpr->columns()) {
    NG_RETURN_IF_ERROR(extractPropExprs(col->expr(), uniqueEdgeVertexExpr));
    newYieldExpr->addColumn(new YieldColumn(rewrite2VarProp(col->expr()), col->alias()));
  }

  goCtx_->yieldExpr = newYieldExpr;
  return Status::OK();
}

bool GoValidator::isSimpleCase() {
  // Check limit clause
  if (!goCtx_->limits.empty()) {
    return false;
  }
  // Check if the filter or yield cluase uses:
  // 1. src tag props,
  // 2. or edge props, except the dst id of edge.
  // 3. input or var props.
  auto& exprProps = goCtx_->exprProps;
  if (!exprProps.srcTagProps().empty()) return false;
  if (!exprProps.edgeProps().empty()) {
    for (auto& edgeProp : exprProps.edgeProps()) {
      auto props = edgeProp.second;
      if (props.size() != 1) return false;
      if (props.find(kDst) == props.end()) return false;
    }
  }
  if (exprProps.hasInputVarProperty()) return false;

  // Check filter clause
  // Because GetDstBySrc doesn't support filter push down,
  // so we don't optimize such case.
  if (goCtx_->filter) {
    if (ExpressionUtils::findEdgeDstExpr(goCtx_->filter)) {
      return false;
    }
  }

  // Check yield clause
  if (!goCtx_->distinct) return false;
  bool atLeastOneDstId = false;
  for (auto& col : goCtx_->yieldExpr->columns()) {
    auto expr = col->expr();
    if (expr->kind() != Expression::Kind::kEdgeDst) continue;
    atLeastOneDstId = true;
    auto dstIdExpr = static_cast<const EdgeDstIdExpression*>(expr);
    if (dstIdExpr->sym() != "*" && goCtx_->over.edgeTypes.size() != 1) {
      return false;
    }
  }
  return atLeastOneDstId;
}

void GoValidator::rewrite2EdgeDst() {
  auto matcher = [](const Expression* e) -> bool {
    if (e->kind() != Expression::Kind::kEdgeDst) {
      return false;
    }
    auto* edgeDstExpr = static_cast<const EdgeDstIdExpression*>(e);
    return edgeDstExpr->sym() != "*";
  };
  auto rewriter = [this](const Expression*) -> Expression* {
    return EdgeDstIdExpression::make(qctx_->objPool(), "*");
  };

  if (goCtx_->filter != nullptr) {
    goCtx_->filter = RewriteVisitor::transform(goCtx_->filter, matcher, rewriter);
  }
  auto* newYieldExpr = qctx_->objPool()->makeAndAdd<YieldColumns>();
  for (auto* col : goCtx_->yieldExpr->columns()) {
    newYieldExpr->addColumn(
        new YieldColumn(RewriteVisitor::transform(col->expr(), matcher, rewriter), col->alias()));
  }
  goCtx_->yieldExpr = newYieldExpr;
}

void GoValidator::rewriteEdgeDst2VarProp() {
  auto matcher = [](const Expression* expr) { return expr->kind() == Expression::Kind::kEdgeDst; };
  auto rewriter = [this](const Expression*) {
    return VariablePropertyExpression::make(qctx_->objPool(), "", goCtx_->dstIdColName);
  };
  if (goCtx_->filter != nullptr) {
    goCtx_->filter = RewriteVisitor::transform(goCtx_->filter, matcher, rewriter);
  }
  auto* newYieldExpr = qctx_->objPool()->makeAndAdd<YieldColumns>();
  for (auto* col : goCtx_->yieldExpr->columns()) {
    newYieldExpr->addColumn(
        new YieldColumn(RewriteVisitor::transform(col->expr(), matcher, rewriter), col->alias()));
  }
  goCtx_->yieldExpr = newYieldExpr;
}

}  // namespace graph
}  // namespace nebula
