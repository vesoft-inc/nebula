/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/validator/FetchVerticesValidator.h"

#include <string>         // for operator==, string
#include <unordered_map>  // for _Node_const_iterator
#include <utility>        // for pair

#include "common/base/Base.h"                      // for kTag, kVid
#include "common/base/ObjectPool.h"                // for ObjectPool
#include "common/base/StatusOr.h"                  // for StatusOr
#include "common/expression/Expression.h"          // for Expression::Kind
#include "common/expression/PropertyExpression.h"  // for InputPropertyExpre...
#include "common/meta/SchemaManager.h"             // for SchemaManager
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/Symbols.h"                 // for ColsDef
#include "graph/session/ClientSession.h"           // for SpaceInfo
#include "graph/util/ExpressionUtils.h"            // for ExpressionUtils
#include "graph/util/ValidateUtil.h"               // for ValidateUtil
#include "graph/visitor/DeducePropsVisitor.h"      // for ExpressionProps
#include "parser/Clauses.h"                        // for YieldColumn, Yield...
#include "parser/TraverseSentences.h"              // for FetchVerticesSentence

namespace nebula {
namespace graph {

Status FetchVerticesValidator::validateImpl() {
  auto *fSentence = static_cast<FetchVerticesSentence *>(sentence_);
  fetchCtx_ = getContext<FetchVerticesContext>();
  fetchCtx_->inputVarName = inputVarName_;

  NG_RETURN_IF_ERROR(validateTag(fSentence->tags()));
  NG_RETURN_IF_ERROR(validateStarts(fSentence->vertices(), fetchCtx_->from));
  NG_RETURN_IF_ERROR(validateYield(fSentence->yieldClause()));
  return Status::OK();
}

Status FetchVerticesValidator::validateTag(const NameLabelList *nameLabels) {
  if (nameLabels == nullptr) {
    // all tag
    const auto &tagStatus = qctx_->schemaMng()->getAllLatestVerTagSchema(space_.id);
    NG_RETURN_IF_ERROR(tagStatus);
    for (const auto &tag : tagStatus.value()) {
      tagsSchema_.emplace(tag.first, tag.second);
      tagIds_.emplace_back(tag.first);
    }
  } else {
    auto labels = nameLabels->labels();
    auto *schemaMng = qctx_->schemaMng();
    for (const auto &label : labels) {
      auto tagStatus = schemaMng->toTagID(space_.id, *label);
      NG_RETURN_IF_ERROR(tagStatus);
      auto tagID = tagStatus.value();
      auto tagSchema = schemaMng->getTagSchema(space_.id, tagID);
      if (tagSchema == nullptr) {
        return Status::SemanticError("no schema found for `%s'", label->c_str());
      }
      tagsSchema_.emplace(tagID, tagSchema);
      tagIds_.emplace_back(tagID);
    }
  }
  return Status::OK();
}

Status FetchVerticesValidator::validateYield(YieldClause *yield) {
  if (yield == nullptr) {
    return Status::SemanticError("Missing yield clause.");
  }
  fetchCtx_->distinct = yield->isDistinct();
  auto size = yield->columns().size();
  outputs_.reserve(size);

  auto pool = qctx_->objPool();
  auto *newCols = pool->add(new YieldColumns());

  auto &exprProps = fetchCtx_->exprProps;
  for (auto col : yield->columns()) {
    if (ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kEdge})) {
      return Status::SemanticError("illegal yield clauses `%s'", col->toString().c_str());
    }
    col->setExpr(ExpressionUtils::rewriteLabelAttr2TagProp(col->expr()));
    NG_RETURN_IF_ERROR(ValidateUtil::invalidLabelIdentifiers(col->expr()));

    auto colExpr = col->expr();
    auto typeStatus = deduceExprType(colExpr);
    NG_RETURN_IF_ERROR(typeStatus);
    outputs_.emplace_back(col->name(), typeStatus.value());
    if (colExpr->toString() == "id(VERTEX)") {
      col->setAlias(col->name());
      col->setExpr(InputPropertyExpression::make(pool, ::nebula::kVid));
    }
    newCols->addColumn(col->clone().release());
    NG_RETURN_IF_ERROR(deduceProps(colExpr, exprProps, &tagIds_));
  }
  if (exprProps.tagProps().empty()) {
    for (const auto &tagSchema : tagsSchema_) {
      exprProps.insertTagProp(tagSchema.first, ::nebula::kTag);
    }
  }
  fetchCtx_->yieldExpr = newCols;

  if (exprProps.hasInputVarProperty()) {
    return Status::SemanticError("unsupported input/variable property expression in yield.");
  }
  if (exprProps.hasSrcDstTagProperty()) {
    return Status::SemanticError("unsupported src/dst property expression in yield.");
  }

  for (const auto &tag : exprProps.tagNameIds()) {
    if (tagsSchema_.find(tag.second) == tagsSchema_.end()) {
      return Status::SemanticError("mismatched tag `%s'", tag.first.c_str());
    }
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
