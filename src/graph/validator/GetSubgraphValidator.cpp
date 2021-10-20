/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/validator/GetSubgraphValidator.h"

#include <memory>

#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ValidateUtil.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
  auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
  subgraphCtx_ = getContext<SubgraphContext>();
  subgraphCtx_->withProp = gsSentence->withProp();

  NG_RETURN_IF_ERROR(ValidateUtil::validateStep(gsSentence->step(), subgraphCtx_->steps));
  NG_RETURN_IF_ERROR(validateStarts(gsSentence->from(), subgraphCtx_->from));
  NG_RETURN_IF_ERROR(validateInBound(gsSentence->in()));
  NG_RETURN_IF_ERROR(validateOutBound(gsSentence->out()));
  NG_RETURN_IF_ERROR(validateBothInOutBound(gsSentence->both()));
  NG_RETURN_IF_ERROR(validateYield(gsSentence->yield()));
  return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  if (in != nullptr) {
    auto space = vctx_->whichSpace();
    auto edges = in->edges();
    edgeTypes.reserve(edgeTypes.size() + edges.size());
    for (auto* e : edges) {
      if (e->alias() != nullptr) {
        return Status::SemanticError("Get Subgraph not support rename edge name.");
      }

      auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
      NG_RETURN_IF_ERROR(et);

      auto v = -et.value();
      edgeTypes.emplace(v);
    }
  }

  return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  if (out != nullptr) {
    auto space = vctx_->whichSpace();
    auto edges = out->edges();
    edgeTypes.reserve(edgeTypes.size() + edges.size());
    for (auto* e : out->edges()) {
      if (e->alias() != nullptr) {
        return Status::SemanticError("Get Subgraph not support rename edge name.");
      }

      auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
      NG_RETURN_IF_ERROR(et);

      edgeTypes.emplace(et.value());
    }
  }

  return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  if (out != nullptr) {
    auto space = vctx_->whichSpace();
    auto edges = out->edges();
    edgeTypes.reserve(edgeTypes.size() + edges.size());
    for (auto* e : out->edges()) {
      if (e->alias() != nullptr) {
        return Status::SemanticError("Get Subgraph not support rename edge name.");
      }

      auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
      NG_RETURN_IF_ERROR(et);

      auto v = et.value();
      edgeTypes.emplace(v);
      v = -v;
      edgeTypes.emplace(v);
    }
  }
  return Status::OK();
}

Status GetSubgraphValidator::validateYield(YieldClause* yield) {
  auto pool = qctx_->objPool();
  if (yield == nullptr) {
    // version 3.0: return Status::SemanticError("No Yield Clause");
    auto* yieldColumns = new YieldColumns();
    auto* vertex = new YieldColumn(LabelExpression::make(pool, "_vertices"));
    yieldColumns->addColumn(vertex);
    if (subgraphCtx_->steps.steps() != 0) {
      auto* edge = new YieldColumn(LabelExpression::make(pool, "_edges"));
      yieldColumns->addColumn(edge);
    }
    yield = pool->add(new YieldClause(yieldColumns));
  }
  auto size = yield->columns().size();
  outputs_.reserve(size);
  YieldColumns* newCols = pool->add(new YieldColumns());

  for (const auto& col : yield->columns()) {
    std::string lowerStr = col->expr()->toString();
    folly::toLowerAscii(lowerStr);
    if (lowerStr == "vertices" || lowerStr == "_vertices") {
      subgraphCtx_->getVertexProp = true;
      auto* newCol = new YieldColumn(InputPropertyExpression::make(pool, "VERTICES"), col->name());
      newCols->addColumn(newCol);
    } else if (lowerStr == "edges" || lowerStr == "_edges") {
      if (subgraphCtx_->steps.steps() == 0) {
        return Status::SemanticError("Get Subgraph 0 STEPS only support YIELD vertices");
      }
      subgraphCtx_->getEdgeProp = true;
      auto* newCol = new YieldColumn(InputPropertyExpression::make(pool, "EDGES"), col->name());
      newCols->addColumn(newCol);
    } else {
      return Status::SemanticError("Get Subgraph only support YIELD vertices OR edges");
    }
    outputs_.emplace_back(col->name(), Value::Type::LIST);
  }
  subgraphCtx_->yieldExpr = newCols;
  subgraphCtx_->colNames = getOutColNames();
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
