/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/validator/FetchEdgesValidator.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/ValidateUtil.h"

namespace nebula {
namespace graph {

Status FetchEdgesValidator::validateImpl() {
  auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
  if (sentence->edgeSize() != 1) {
    return Status::SemanticError("only allow fetch on one edge");
  }
  edgeName_ = sentence->edgeName();
  fetchCtx_ = getContext<FetchEdgesContext>();
  NG_RETURN_IF_ERROR(validateEdgeName());
  NG_RETURN_IF_ERROR(validateEdgeKey());
  NG_RETURN_IF_ERROR(validateYield(sentence->yieldClause()));
  fetchCtx_->edgeName = std::move(edgeName_);
  return Status::OK();
}

Status FetchEdgesValidator::validateEdgeName() {
  auto &spaceID = space_.id;
  auto status = qctx_->schemaMng()->toEdgeType(spaceID, edgeName_);
  NG_RETURN_IF_ERROR(status);
  edgeType_ = status.value();
  edgeSchema_ = qctx_->schemaMng()->getEdgeSchema(spaceID, edgeType_);
  if (edgeSchema_ == nullptr) {
    return Status::SemanticError("No schema found for `%s'", edgeName_.c_str());
  }
  return Status::OK();
}

StatusOr<std::string> FetchEdgesValidator::validateEdgeRef(const Expression *expr,
                                                           Value::Type type) {
  const auto &kind = expr->kind();
  if (kind != Expression::Kind::kInputProperty && kind != EdgeExpression::Kind::kVarProperty) {
    return Status::SemanticError("`%s', only input and variable expression is acceptable",
                                 expr->toString().c_str());
  }
  auto exprType = deduceExprType(expr);
  NG_RETURN_IF_ERROR(exprType);
  if (exprType.value() != type) {
    std::stringstream ss;
    ss << "`" << expr->toString() << "' should be type of " << type << ", but was "
       << exprType.value();
    return Status::SemanticError(ss.str());
  }
  if (kind == Expression::Kind::kInputProperty) {
    return inputVarName_;
  }
  const auto *propExpr = static_cast<const PropertyExpression *>(expr);
  userDefinedVarNameList_.emplace(propExpr->sym());
  return propExpr->sym();
}

Status FetchEdgesValidator::validateEdgeKey() {
  auto pool = qctx_->objPool();
  auto *sentence = static_cast<FetchEdgesSentence *>(sentence_);
  std::string inputVarName;
  if (sentence->isRef()) {
    auto *srcExpr = sentence->ref()->srcid();
    auto result = validateEdgeRef(srcExpr, vidType_);
    NG_RETURN_IF_ERROR(result);
    inputVarName = std::move(result).value();

    auto *rankExpr = sentence->ref()->rank();
    if (rankExpr->kind() != Expression::Kind::kConstant) {
      result = validateEdgeRef(rankExpr, Value::Type::INT);
      NG_RETURN_IF_ERROR(result);
      if (inputVarName != result.value()) {
        return Status::SemanticError(
            "`%s' the src dst and rank of the edge must use the same reference.",
            rankExpr->toString().c_str());
      }
    }

    auto *dstExpr = sentence->ref()->dstid();
    result = validateEdgeRef(dstExpr, vidType_);
    NG_RETURN_IF_ERROR(result);
    if (inputVarName != result.value()) {
      return Status::SemanticError(
          "`%s' the src dst and rank of the edge must use the same reference.",
          dstExpr->toString().c_str());
    }
    fetchCtx_->src = srcExpr;
    fetchCtx_->dst = dstExpr;
    fetchCtx_->rank = rankExpr;
    fetchCtx_->type = ConstantExpression::make(pool, edgeType_);
    fetchCtx_->inputVarName = std::move(inputVarName);
    return Status::OK();
  }

  DataSet edgeKeys{{kSrc, kRank, kDst}};
  QueryExpressionContext ctx;
  auto keys = sentence->keys()->keys();
  edgeKeys.rows.reserve(keys.size());
  for (const auto &key : keys) {
    if (!evaluableExpr(key->srcid())) {
      return Status::SemanticError("`%s' is not evaluable.", key->srcid()->toString().c_str());
    }
    auto src = key->srcid()->eval(ctx);
    if (src.type() != vidType_) {
      std::stringstream ss;
      ss << "the src should be type of " << vidType_ << ", but was`" << src.type() << "'";
      return Status::SemanticError(ss.str());
    }
    auto ranking = key->rank();

    if (!evaluableExpr(key->dstid())) {
      return Status::SemanticError("`%s' is not evaluable.", key->dstid()->toString().c_str());
    }
    auto dst = key->dstid()->eval(ctx);
    if (dst.type() != vidType_) {
      std::stringstream ss;
      ss << "the dst should be type of " << vidType_ << ", but was`" << dst.type() << "'";
      return Status::SemanticError(ss.str());
    }
    edgeKeys.emplace_back(nebula::Row({std::move(src), ranking, std::move(dst)}));
  }
  inputVarName = vctx_->anonVarGen()->getVar();
  qctx_->ectx()->setResult(inputVarName, ResultBuilder().value(Value(std::move(edgeKeys))).build());
  fetchCtx_->src = ColumnExpression::make(pool, 0);
  fetchCtx_->rank = ColumnExpression::make(pool, 1);
  fetchCtx_->dst = ColumnExpression::make(pool, 2);
  fetchCtx_->type = ConstantExpression::make(pool, edgeType_);
  fetchCtx_->inputVarName = std::move(inputVarName);
  return Status::OK();
}

void FetchEdgesValidator::extractEdgeProp(ExpressionProps &exprProps) {
  exprProps.insertEdgeProp(edgeType_, kSrc);
  exprProps.insertEdgeProp(edgeType_, kDst);
  exprProps.insertEdgeProp(edgeType_, kRank);
  exprProps.insertEdgeProp(edgeType_, kType);

  for (std::size_t i = 0; i < edgeSchema_->getNumFields(); ++i) {
    const auto propName = edgeSchema_->getFieldName(i);
    exprProps.insertEdgeProp(edgeType_, propName);
  }
}

Status FetchEdgesValidator::validateYield(const YieldClause *yield) {
  auto pool = qctx_->objPool();
  bool noYield = false;
  if (yield == nullptr) {
    // TODO: compatible with previous version, this will be deprecated in version 3.0.
    auto *yieldColumns = new YieldColumns();
    auto *edge = new YieldColumn(EdgeExpression::make(pool), "edges_");
    yieldColumns->addColumn(edge);
    yield = pool->add(new YieldClause(yieldColumns));
    noYield = true;
  }
  fetchCtx_->distinct = yield->isDistinct();

  auto &exprProps = fetchCtx_->exprProps;
  auto *newCols = pool->add(new YieldColumns());
  if (!noYield) {
    auto *src = new YieldColumn(EdgeSrcIdExpression::make(pool, edgeName_));
    auto *dst = new YieldColumn(EdgeDstIdExpression::make(pool, edgeName_));
    auto *rank = new YieldColumn(EdgeRankExpression::make(pool, edgeName_));
    outputs_.emplace_back(src->name(), vidType_);
    outputs_.emplace_back(dst->name(), vidType_);
    outputs_.emplace_back(rank->name(), Value::Type::INT);
    newCols->addColumn(src);
    newCols->addColumn(dst);
    newCols->addColumn(rank);
    exprProps.insertEdgeProp(edgeType_, kSrc);
    exprProps.insertEdgeProp(edgeType_, kDst);
    exprProps.insertEdgeProp(edgeType_, kRank);
  }

  for (const auto &col : yield->columns()) {
    if (ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kEdge})) {
      extractEdgeProp(exprProps);
      break;
    }
  }
  auto size = yield->columns().size();
  outputs_.reserve(size + 3);

  for (auto col : yield->columns()) {
    if (ExpressionUtils::hasAny(col->expr(),
                                {Expression::Kind::kVertex, Expression::Kind::kPathBuild})) {
      return Status::SemanticError("illegal yield clauses `%s'", col->toString().c_str());
    }
    col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));
    NG_RETURN_IF_ERROR(ValidateUtil::invalidLabelIdentifiers(col->expr()));

    auto colExpr = col->expr();
    auto typeStatus = deduceExprType(colExpr);
    NG_RETURN_IF_ERROR(typeStatus);
    outputs_.emplace_back(col->name(), typeStatus.value());
    newCols->addColumn(col->clone().release());

    NG_RETURN_IF_ERROR(deduceProps(colExpr, exprProps));
  }

  if (exprProps.hasInputVarProperty()) {
    return Status::SemanticError("unsupported input/variable property expression in yield.");
  }

  if (exprProps.hasSrcDstTagProperty()) {
    return Status::SemanticError("unsupported src/dst property expression in yield.");
  }

  for (const auto &edge : exprProps.edgeProps()) {
    if (edge.first != edgeType_) {
      return Status::SemanticError("should use edge name `%s'", edgeName_.c_str());
    }
  }
  fetchCtx_->yieldExpr = newCols;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
