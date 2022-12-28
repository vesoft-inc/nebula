/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/LookupPlanner.h"

#include "common/base/Base.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Scan.h"
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
  SubPlan plan;
  if (lookupCtx->isFulltextIndex) {
    auto expr = static_cast<TextSearchExpression*>(lookupCtx->fulltextExpr);
    auto fulltextIndexScan =
        FulltextIndexScan::make(qctx, lookupCtx->fulltextIndex, expr, lookupCtx->isEdge);
    plan.tail = fulltextIndexScan;
    plan.root = fulltextIndexScan;

    if (lookupCtx->isEdge) {
      auto* pool = qctx->objPool();
      storage::cpp2::EdgeProp edgeProp;
      edgeProp.type_ref() = lookupCtx->schemaId;
      edgeProp.props_ref() = lookupCtx->idxReturnCols;
      auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
      edgeProps->emplace_back(std::move(edgeProp));
      auto getEdges = GetEdges::make(qctx,
                                     fulltextIndexScan,
                                     lookupCtx->space.id,
                                     ColumnExpression::make(pool, 0),
                                     ConstantExpression::make(pool, Value(lookupCtx->schemaId)),
                                     ColumnExpression::make(pool, 1),
                                     ColumnExpression::make(pool, 2),
                                     std::move(edgeProps));
      plan.root = getEdges;
    } else {
      auto* pool = qctx->objPool();
      storage::cpp2::VertexProp vertexProp;
      vertexProp.tag_ref() = lookupCtx->schemaId;
      vertexProp.props_ref() = lookupCtx->idxReturnCols;
      auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
      vertexProps->emplace_back(std::move(vertexProp));
      auto getVertices = GetVertices::make(qctx,
                                           fulltextIndexScan,
                                           lookupCtx->space.id,
                                           ColumnExpression::make(pool, 0),
                                           std::move(vertexProps));
      plan.root = getVertices;
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
