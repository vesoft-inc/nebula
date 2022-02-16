/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownIndexScanRule.h"

#include <stdint.h>  // for int64_t

#include <utility>  // for move
#include <vector>   // for vector

#include "graph/optimizer/OptContext.h"   // for OptContext
#include "graph/optimizer/OptGroup.h"     // for OptGroupNode, OptGroup
#include "graph/planner/plan/PlanNode.h"  // for PlanNode, PlanNode::Kind
#include "graph/planner/plan/Query.h"     // for IndexScan, Limit, Project (...

namespace nebula {
namespace graph {
class QueryContext;

class QueryContext;
}  // namespace graph
}  // namespace nebula

using nebula::graph::IndexScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

/*static*/ const std::initializer_list<graph::PlanNode::Kind>
    PushLimitDownIndexScanRule::kIndexScanKinds{
        graph::PlanNode::Kind::kIndexScan,
        graph::PlanNode::Kind::kTagIndexFullScan,
        graph::PlanNode::Kind::kTagIndexRangeScan,
        graph::PlanNode::Kind::kTagIndexPrefixScan,
        graph::PlanNode::Kind::kEdgeIndexFullScan,
        graph::PlanNode::Kind::kEdgeIndexRangeScan,
        graph::PlanNode::Kind::kEdgeIndexPrefixScan,
    };

std::unique_ptr<OptRule> PushLimitDownIndexScanRule::kInstance =
    std::unique_ptr<PushLimitDownIndexScanRule>(new PushLimitDownIndexScanRule());

PushLimitDownIndexScanRule::PushLimitDownIndexScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownIndexScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kLimit, {Pattern::create(kIndexScanKinds)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownIndexScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto indexScanGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto indexScan = indexScanGroupNode->node()->asNode<graph::IndexScan>();

  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (indexScan->limit(qctx) >= 0 && limitRows >= indexScan->limit(qctx)) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newIndexScan = static_cast<IndexScan *>(indexScan->clone());
  newIndexScan->setLimit(limitRows);
  auto newIndexScanGroup = OptGroup::create(octx);
  auto newIndexScanGroupNode = newIndexScanGroup->makeGroupNode(newIndexScan);

  newLimitGroupNode->dependsOn(newIndexScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newIndexScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownIndexScanRule::toString() const {
  return "PushLimitDownIndexScanRule";
}

}  // namespace opt
}  // namespace nebula
