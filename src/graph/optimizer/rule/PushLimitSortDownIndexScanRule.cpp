/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushLimitSortDownIndexScanRule.h"

#include "common/expression/FunctionCallExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"

using nebula::graph::IndexScan;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::TagIndexFullScan;
using nebula::graph::TopN;
using nebula::storage::cpp2::IndexQueryContext;

namespace nebula {
namespace opt {

StatusOr<OptRule::TransformResult> PushLimitSortDownIndexScanRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  int64_t limitRows = limit(matched);
  auto ret = orderBy(matched);
  if (!ret.ok()) {
    return TransformResult::noTransform();
  }
  std::vector<storage::cpp2::OrderBy> orderBys = std::move(ret.value());

  auto newTopNGroupNode = topN(ctx, matched);

  auto newProjGroupNode = project(ctx, matched);

  auto indexScanGroupNode = matched.dependencies.front().node;
  if (newProjGroupNode != nullptr) {
    indexScanGroupNode = matched.dependencies.front().dependencies.front().node;
  }
  const auto indexScan = static_cast<const graph::IndexScan *>(indexScanGroupNode->node());
  auto newIndexScan = static_cast<graph::IndexScan *>(indexScan->clone());
  newIndexScan->setLimit(limitRows);
  newIndexScan->setOrderBy(std::move(orderBys));
  auto newIndexGroup = OptGroup::create(ctx);
  auto newIndexGroupNode = newIndexGroup->makeGroupNode(newIndexScan);

  if (newProjGroupNode != nullptr) {
    newTopNGroupNode->dependsOn(const_cast<opt::OptGroup *>(newProjGroupNode->group()));
    newProjGroupNode->dependsOn(newIndexGroup);
  } else {
    newTopNGroupNode->dependsOn(newIndexGroup);
  }

  for (auto dep : indexScanGroupNode->dependencies()) {
    newIndexGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newTopNGroupNode);
  return result;
}

}  // namespace opt
}  // namespace nebula
