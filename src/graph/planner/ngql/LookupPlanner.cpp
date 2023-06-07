/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/LookupPlanner.h"

#include "common/base/Base.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Scan.h"
#include "graph/util/Constants.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

std::unique_ptr<Planner> LookupPlanner::make() {
  return std::unique_ptr<LookupPlanner>(new LookupPlanner());
}

bool LookupPlanner::match(AstContext* astCtx) {
  return astCtx->sentence->kind() == Sentence::Kind::kLookup;
}

StatusOr<SubPlan> LookupPlanner::transform(AstContext* astCtx) {
  auto lookupCtx = static_cast<LookupContext*>(astCtx);
  auto qctx = lookupCtx->qctx;
  auto from = static_cast<const LookupSentence*>(lookupCtx->sentence)->from();
  const char* kIdColName = "id";
  SubPlan plan;
  if (lookupCtx->isFulltextIndex) {
    auto expr = static_cast<TextSearchExpression*>(lookupCtx->fulltextExpr);
    auto fulltextIndexScan =
        FulltextIndexScan::make(qctx, expr, lookupCtx->isEdge, lookupCtx->schemaId);
    fulltextIndexScan->setColNames({kIdColName});
    plan.tail = fulltextIndexScan;
    plan.root = fulltextIndexScan;

    if (lookupCtx->hasScore) {
      auto argument = Argument::make(qctx, kIdColName);
      argument->setInputVertexRequired(false);
      plan.root = argument;
    }

    auto* pool = qctx->objPool();
    auto spaceId = lookupCtx->space.id;

    std::vector<Expression*> hashKeys, probeKeys;

    if (lookupCtx->isEdge) {
      storage::cpp2::EdgeProp edgeProp;
      edgeProp.type_ref() = lookupCtx->schemaId;
      edgeProp.props_ref() = lookupCtx->idxReturnCols;
      auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
      edgeProps->emplace_back(std::move(edgeProp));

      auto src = FunctionCallExpression::make(pool, "src", {ColumnExpression::make(pool, 0)});
      auto type = FunctionCallExpression::make(pool, "typeid", {ColumnExpression::make(pool, 0)});
      auto rank = FunctionCallExpression::make(pool, "rank", {ColumnExpression::make(pool, 0)});
      auto dst = FunctionCallExpression::make(pool, "dst", {ColumnExpression::make(pool, 0)});

      plan.root =
          GetEdges::make(qctx, plan.root, spaceId, src, type, rank, dst, std::move(edgeProps));

      hashKeys = {ColumnExpression::make(pool, 0)};
      probeKeys = {ColumnExpression::make(pool, 0)};
    } else {
      storage::cpp2::VertexProp vertexProp;
      vertexProp.tag_ref() = lookupCtx->schemaId;
      vertexProp.props_ref() = lookupCtx->idxReturnCols;
      auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
      vertexProps->emplace_back(std::move(vertexProp));
      plan.root = GetVertices::make(
          qctx, plan.root, spaceId, ColumnExpression::make(pool, 0), std::move(vertexProps));

      hashKeys = {ColumnExpression::make(pool, 0)};
      probeKeys = {FunctionCallExpression::make(pool, "id", {ColumnExpression::make(pool, 0)})};
    }

    if (lookupCtx->hasScore) {
      plan.root = HashInnerJoin::make(
          qctx, fulltextIndexScan, plan.root, std::move(hashKeys), std::move(probeKeys));
    }
  } else {
    if (lookupCtx->isEdge) {
      auto* edgeIndexFullScan = EdgeIndexFullScan::make(qctx,
                                                        nullptr,
                                                        from,
                                                        lookupCtx->space.id,
                                                        {},
                                                        lookupCtx->idxReturnCols,
                                                        lookupCtx->schemaId);
      edgeIndexFullScan->setYieldColumns(lookupCtx->yieldExpr);
      plan.tail = edgeIndexFullScan;
      plan.root = edgeIndexFullScan;
    } else {
      auto* tagIndexFullScan = TagIndexFullScan::make(qctx,
                                                      nullptr,
                                                      from,
                                                      lookupCtx->space.id,
                                                      {},
                                                      lookupCtx->idxReturnCols,
                                                      lookupCtx->schemaId);
      tagIndexFullScan->setYieldColumns(lookupCtx->yieldExpr);
      plan.tail = tagIndexFullScan;
      plan.root = tagIndexFullScan;
    }
    plan.tail->setColNames(lookupCtx->idxColNames);

    if (lookupCtx->filter) {
      plan.root = Filter::make(qctx, plan.root, lookupCtx->filter);
    }
  }
  plan.root = Project::make(qctx, plan.root, lookupCtx->yieldExpr);
  if (lookupCtx->dedup) {
    plan.root = Dedup::make(qctx, plan.root);
  }

  return plan;
}

}  // namespace graph
}  // namespace nebula
