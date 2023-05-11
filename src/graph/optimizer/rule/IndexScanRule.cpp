/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/IndexScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/OptimizerUtils.h"
#include "graph/visitor/RewriteVisitor.h"

using nebula::graph::IndexScan;
using nebula::graph::OptimizerUtils;
using nebula::storage::cpp2::IndexQueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> IndexScanRule::kInstance =
    std::unique_ptr<IndexScanRule>(new IndexScanRule());

IndexScanRule::IndexScanRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& IndexScanRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kIndexScan);
  return pattern;
}

bool IndexScanRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto scan = static_cast<const IndexScan*>(matched.planNode());
  // Has been optimized, skip this rule
  for (auto& ictx : scan->queryContext()) {
    if (ictx.index_id_ref().is_set()) {
      return false;
    }
  }
  return true;
}

StatusOr<OptRule::TransformResult> IndexScanRule::transform(OptContext* ctx,
                                                            const MatchedResult& matched) const {
  auto groupNode = matched.node;

  auto filter = filterExpr(groupNode);
  auto qctx = ctx->qctx();
  const auto* oldIN = groupNode->node();
  DCHECK_EQ(oldIN->kind(), graph::PlanNode::Kind::kIndexScan);
  auto node = static_cast<const IndexScan*>(oldIN);
  std::vector<IndexQueryContext> iqctx;
  NG_RETURN_IF_ERROR(OptimizerUtils::createIndexQueryCtx(filter, qctx, node, iqctx));

  auto* newIN = static_cast<IndexScan*>(oldIN->clone());
  newIN->setOutputVar(oldIN->outputVar());
  newIN->setIndexQueryContext(std::move(iqctx));
  auto newGroupNode = OptGroupNode::create(ctx, newIN, groupNode->group());
  if (groupNode->dependencies().size() != 1) {
    return Status::Error("Plan node dependencies error");
  }
  newGroupNode->dependsOn(groupNode->dependencies()[0]);
  TransformResult result;
  result.newGroupNodes.emplace_back(newGroupNode);
  result.eraseAll = true;
  return result;
}

std::string IndexScanRule::toString() const {
  return "IndexScanRule";
}

// TODO: Delete similar interfaces and get the filter from PlanNode
Expression* IndexScanRule::filterExpr(const OptGroupNode* groupNode) const {
  auto in = static_cast<const IndexScan*>(groupNode->node());
  const auto& qct = in->queryContext();
  // The initial IndexScan plan node has only zero or one queryContext.
  // TODO(yee): Move this condition to match interface
  if (qct.empty()) {
    return nullptr;
  }

  if (qct.size() != 1) {
    LOG(ERROR) << "Index Scan plan node error";
    return nullptr;
  }
  auto* pool = in->qctx()->objPool();
  return Expression::decode(pool, qct.begin()->get_filter());
}

}  // namespace opt
}  // namespace nebula
