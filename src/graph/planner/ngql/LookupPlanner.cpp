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
  SubPlan plan;
  if (lookupCtx->isFulltextIndex) {
    auto expr = static_cast<TextSearchExpression*>(lookupCtx->fulltextExpr);
    auto fulltextIndexScan = FulltextIndexScan::make(qctx, expr, lookupCtx->isEdge);
    plan.tail = fulltextIndexScan;
    plan.root = fulltextIndexScan;

    if (lookupCtx->hasScore) {
      auto argument = Argument::make(qctx, kScore);
      argument->setInputVar(fulltextIndexScan->outputVar());
      plan.root = argument;
    }

    auto* pool = qctx->objPool();
    auto spaceId = lookupCtx->space.id;
    auto src = ColumnExpression::make(pool, 0);

    std::vector<Expression*> hashKeys, probeKeys;

    if (lookupCtx->isEdge) {
      storage::cpp2::EdgeProp edgeProp;
      edgeProp.type_ref() = lookupCtx->schemaId;
      edgeProp.props_ref() = lookupCtx->idxReturnCols;
      auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
      edgeProps->emplace_back(std::move(edgeProp));
      auto type = ConstantExpression::make(pool, Value(lookupCtx->schemaId));
      auto rank = ColumnExpression::make(pool, 1);
      auto dst = ColumnExpression::make(pool, 2);
      plan.root =
          GetEdges::make(qctx, plan.root, spaceId, src, type, rank, dst, std::move(edgeProps));

      hashKeys = {src, type, rank, dst};
      auto inputExpr = ColumnExpression::make(pool, 0);
      probeKeys = {
          FunctionCallExpression::make(pool, "src", {inputExpr}),
          FunctionCallExpression::make(pool, "type", {inputExpr}),
          FunctionCallExpression::make(pool, "rank", {inputExpr}),
          FunctionCallExpression::make(pool, "dst", {inputExpr}),
      };
    } else {
      storage::cpp2::VertexProp vertexProp;
      vertexProp.tag_ref() = lookupCtx->schemaId;
      vertexProp.props_ref() = lookupCtx->idxReturnCols;
      auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
      vertexProps->emplace_back(std::move(vertexProp));
      plan.root = GetVertices::make(qctx, plan.root, spaceId, src, std::move(vertexProps));

      hashKeys = {src};
      probeKeys = {FunctionCallExpression::make(pool, "id", {ColumnExpression::make(pool, 0)})};
    }

    if (lookupCtx->hasScore) {
      plan.root = HashInnerJoin::make(qctx, fulltextIndexScan, plan.root, hashKeys, probeKeys);
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
