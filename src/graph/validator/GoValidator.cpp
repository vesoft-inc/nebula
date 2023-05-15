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

  NG_RETURN_IF_ERROR(buildColumns());
  return Status::OK();
}

// Validate filter expression, rewrites expression to fit sementic,
// deduce and check the type of expression, collect properties used in filter.
Status GoValidator::validateWhere(WhereClause* where) {
  if (where == nullptr) {
    return Status::OK();
  }

  auto expr = where->filter();
  if (!checkDstPropOrVertexExist(expr)) {
    return Status::SemanticError("`%s' is not support in go sentence.", expr->toString().c_str());
  }

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
    col->setExpr(rewriteVertexEdge2EdgeProp(col->expr()));
    if (!checkDstPropOrVertexExist(col->expr())) {
      return Status::SemanticError("`%s' is not support in go sentence.", col->toString().c_str());
    }

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
  auto pool = qctx_->objPool();
  goCtx_->srcPropsExpr = pool->makeAndAdd<YieldColumns>();
  goCtx_->edgePropsExpr = pool->makeAndAdd<YieldColumns>();
  goCtx_->dstPropsExpr = pool->makeAndAdd<YieldColumns>();
  inputPropCols_ = pool->makeAndAdd<YieldColumns>();

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

bool GoValidator::checkDstPropOrVertexExist(const Expression* expr) {
  auto filterExprs = ExpressionUtils::collectAll(
      expr, {Expression::Kind::kVertex, Expression::Kind::kDstProperty});
  for (auto filterExpr : filterExprs) {
    if (filterExpr->kind() == Expression::Kind::kDstProperty) {
      goCtx_->joinDst = true;
    } else {
      auto& name = static_cast<const VertexExpression*>(filterExpr)->name();
      if (name == "VERTEX") {
        return false;
      }
      if (name == "$$") {
        goCtx_->joinDst = true;
      }
    }
  }
  return true;
}

bool GoValidator::isSimpleCase() {
  if (!goCtx_->limits.empty() || !goCtx_->distinct || goCtx_->filter != nullptr) {
    return false;
  }
  // Check if the yield cluase uses:
  // 1. src tag props,
  // 2. or edge props, except the dst id of edge.
  // 3. input or var props.
  auto& exprProps = goCtx_->exprProps;
  if (!exprProps.srcTagProps().empty() || !exprProps.dstTagProps().empty()) {
    return false;
  }
  if (!exprProps.edgeProps().empty()) {
    for (auto& edgeProp : exprProps.edgeProps()) {
      auto props = edgeProp.second;
      if (props.size() != 1 && props.find(kDst) == props.end()) {
        return false;
      }
    }
  }

  bool atLeastOneDstId = false;
  for (auto& col : goCtx_->yieldExpr->columns()) {
    auto expr = col->expr();
    if (expr->kind() != Expression::Kind::kEdgeDst) {
      return false;
    }
    atLeastOneDstId = true;
    auto dstIdExpr = static_cast<const EdgeDstIdExpression*>(expr);
    if (dstIdExpr->sym() != "*" && goCtx_->over.edgeTypes.size() != 1) {
      return false;
    }
  }
  return atLeastOneDstId;
}

}  // namespace graph
}  // namespace nebula
