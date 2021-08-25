/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "graph/validator/FetchVerticesValidator.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/ValidateUtil.h"
#include "graph/visitor/DeducePropsVisitor.h"

namespace nebula {
namespace graph {

static constexpr char VertexID[] = "VertexID";

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
        return Status::SemanticError("No schema found for `%s'", label->c_str());
      }
      tagsSchema_.emplace(tagID, tagSchema);
    }
  }
  return Status::OK();
}

Status FetchVerticesValidator::validateYield(YieldClause *yield) {
  auto pool = qctx_->objPool();
  if (yield == nullptr) {
    // version 3.0: return Status::SemanticError("No YIELD Clause");
    auto *yieldColumns = new YieldColumns();
    auto *vertex = new YieldColumn(VertexExpression::make(pool));
    yieldColumns->addColumn(vertex);
    yield = pool->add(new YieldClause(yieldColumns));
  }
  fetchCtx_->distinct = yield->isDistinct();

  auto size = yield->columns().size();
  outputs_.reserve(size + 1);  // VertexID
  outputs_.emplace_back(VertexID, vidType_);

  auto &exprProps = fetchCtx_->exprProps;
  for (auto col : yield->columns()) {
    // yield vertex or id(vertex)
    col->setExpr(ExpressionUtils::rewriteLabelAttr2TagProp(col->expr()));
    NG_RETURN_IF_ERROR(ValidateUtil::invalidLabelIdentifiers(col->expr()));

    auto colExpr = col->expr();
    auto typeStatus = deduceExprType(colExpr);
    NG_RETURN_IF_ERROR(typeStatus);
    outputs_.emplace_back(col->name(), typeStatus.value());

    NG_RETURN_IF_ERROR(deduceProps(colExpr, exprProps));
  }

  if (exprProps.hasInputVarProperty()) {
    return Status::SemanticError("Unsupported input/variable property expression in yield.");
  }
  if (exprProps.hasSrcDstTagProperty()) {
    return Status::SemanticError("Unsupported src/dst property expression in yield.");
  }

  // for (const auto &tag : exprProps.tagNameIds()) {
  //   if (tagsSchema_.find(tag.first) == tagsSchema_.end()) {
  //     return Status::SemanticError("Mismatched tag `%s'", tag);
  //   }
  // }
  auto *newCols = pool->add(new YieldColumns());
  // TODO (will be deleted in version 3.0)
  auto *vidCol = new YieldColumn(InputPropertyExpression::make(pool, nebula::kVid), VertexID);
  newCols->addColumn(vidCol);
  for (const auto &col : yield->columns()) {
    newCols->addColumn(col->clone().release());
  }
  fetchCtx_->yieldExpr = newCols;
  auto colNames = getOutColNames();
  colNames.insert(colNames.begin(), nebula::kVid);
  fetchCtx_->colNames = std::move(colNames);
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
