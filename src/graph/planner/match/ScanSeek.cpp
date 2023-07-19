/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/ScanSeek.h"

#include <vector>

#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"

namespace nebula {
namespace graph {

bool ScanSeek::matchEdge(EdgeContext *edgeCtx) {
  UNUSED(edgeCtx);
  return false;
}

StatusOr<SubPlan> ScanSeek::transformEdge(EdgeContext *edgeCtx) {
  UNUSED(edgeCtx);
  return Status::Error("Unimplemented for edge pattern.");
}

bool ScanSeek::matchNode(NodeContext *nodeCtx) {
  auto &node = *nodeCtx->info;
  // only require the tag
  if (node.tids.empty()) {
    // empty labels means all labels
    const auto *qctx = nodeCtx->qctx;
    auto allLabels = qctx->schemaMng()->getAllTags(nodeCtx->spaceId);
    if (!allLabels.ok()) {
      return false;
    }
    for (const auto &label : allLabels.value()) {
      nodeCtx->scanInfo.schemaIds.emplace_back(label.first);
      nodeCtx->scanInfo.schemaNames.emplace_back(label.second);
    }
    nodeCtx->scanInfo.anyLabel = true;
  } else {
    for (std::size_t i = 0; i < node.tids.size(); i++) {
      auto tagId = node.tids[i];
      auto tagName = node.labels[i];
      nodeCtx->scanInfo.schemaIds.emplace_back(tagId);
      nodeCtx->scanInfo.schemaNames.emplace_back(tagName);
    }
  }
  return true;
}

StatusOr<SubPlan> ScanSeek::transformNode(NodeContext *nodeCtx) {
  SubPlan plan;
  auto *qctx = nodeCtx->qctx;
  auto *pool = qctx->objPool();
  auto anyLabel = nodeCtx->scanInfo.anyLabel;

  auto vProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
  std::vector<std::string> colNames{kVid};
  for (std::size_t i = 0; i < nodeCtx->scanInfo.schemaIds.size(); ++i) {
    storage::cpp2::VertexProp vProp;
    std::vector<std::string> props{kTag};
    vProp.tag_ref() = nodeCtx->scanInfo.schemaIds[i];
    vProp.props_ref() = std::move(props);
    vProps->emplace_back(std::move(vProp));
    colNames.emplace_back(nodeCtx->scanInfo.schemaNames[i] + "." + kTag);
  }

  auto *scanVertices = ScanVertices::make(qctx, nullptr, nodeCtx->spaceId, std::move(vProps));
  scanVertices->setColNames(std::move(colNames));
  plan.root = scanVertices;
  plan.tail = scanVertices;

  // Filter vertices lack labels
  Expression *prev = nullptr;
  for (const auto &tag : nodeCtx->scanInfo.schemaNames) {
    auto *tagPropExpr = TagPropertyExpression::make(pool, tag, kTag);
    auto *notEmpty = UnaryExpression::makeIsNotEmpty(pool, tagPropExpr);
    if (prev != nullptr) {
      if (anyLabel) {
        auto *orExpr = LogicalExpression::makeOr(pool, prev, notEmpty);
        prev = orExpr;
      } else {
        auto *andExpr = LogicalExpression::makeAnd(pool, prev, notEmpty);
        prev = andExpr;
      }
    } else {
      prev = notEmpty;
    }
  }
  if (prev != nullptr) {
    // prev equals to nullptr happened when there are no tags in whole space
    auto *filter = Filter::make(qctx, scanVertices, prev);
    plan.root = filter;
  }

  nodeCtx->initialExpr = InputPropertyExpression::make(pool, kVid);
  return plan;
}

}  // namespace graph
}  // namespace nebula
