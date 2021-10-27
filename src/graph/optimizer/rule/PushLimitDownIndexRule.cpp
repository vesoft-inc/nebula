/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushLimitDownIndexRule.h"

#include "common/expression/FunctionCallExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"

using nebula::graph::IndexScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::TagIndexFullScan;
using nebula::storage::cpp2::IndexQueryContext;

namespace nebula {
namespace opt {

int64_t PushLimitDownIndexRule::limit(const MatchedResult &matched) const {
  auto limit = static_cast<const Limit *>(matched.node->node());
  int limitRows = limit->offset() + limit->count();
  auto indexScan = static_cast<const IndexScan *>(matched.planNode({0, 0}));
  if (indexScan->limit() >= 0 && limitRows >= indexScan->limit()) {
    limitRows = indexScan->limit();
  }
  return limitRows;
}
StatusOr<std::vector<storage::cpp2::OrderBy>> PushLimitDownIndexRule::orderBy(
    const MatchedResult &matched) const {
  UNUSED(matched);
  std::vector<storage::cpp2::OrderBy> orderBys;
  return orderBys;
}
OptGroupNode *PushLimitDownIndexRule::topN(OptContext *ctx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  auto newLimit = static_cast<Limit *>(limit->clone());
  return OptGroupNode::create(ctx, newLimit, limitGroupNode->group());
}
OptGroupNode *PushLimitDownIndexRule::project(OptContext *ctx, const MatchedResult &matched) const {
  UNUSED(ctx);
  UNUSED(matched);
  return nullptr;
}

std::unique_ptr<OptRule> PushLimitDownTagIndexFullScanRule::kInstance =
    std::unique_ptr<PushLimitDownTagIndexFullScanRule>(new PushLimitDownTagIndexFullScanRule());
PushLimitDownTagIndexFullScanRule::PushLimitDownTagIndexFullScanRule() {
  RuleSet::QueryRules().addRule(this);
}
const Pattern &PushLimitDownTagIndexFullScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kTagIndexFullScan)});
  return pattern;
}
std::string PushLimitDownTagIndexFullScanRule::toString() const {
  return "PushLimitDownTagIndexFullScanRule";
}

std::unique_ptr<OptRule> PushLimitDownTagIndexPrefixScanRule::kInstance =
    std::unique_ptr<PushLimitDownTagIndexPrefixScanRule>(new PushLimitDownTagIndexPrefixScanRule());
PushLimitDownTagIndexPrefixScanRule::PushLimitDownTagIndexPrefixScanRule() {
  RuleSet::QueryRules().addRule(this);
}
const Pattern &PushLimitDownTagIndexPrefixScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kTagIndexPrefixScan)});
  return pattern;
}  // namespace opt
std::string PushLimitDownTagIndexPrefixScanRule::toString() const {
  return "PushLimitDownTagIndexPrefixScanRule";
}

std::unique_ptr<OptRule> PushLimitDownTagIndexRangeScanRule::kInstance =
    std::unique_ptr<PushLimitDownTagIndexRangeScanRule>(new PushLimitDownTagIndexRangeScanRule());
PushLimitDownTagIndexRangeScanRule::PushLimitDownTagIndexRangeScanRule() {
  RuleSet::QueryRules().addRule(this);
}
const Pattern &PushLimitDownTagIndexRangeScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kTagIndexRangeScan)});
  return pattern;
}  // namespace nebula
std::string PushLimitDownTagIndexRangeScanRule::toString() const {
  return "PushLimitDownTagIndexRangeScanRule";
}

std::unique_ptr<OptRule> PushLimitDownEdgeIndexFullScanRule::kInstance =
    std::unique_ptr<PushLimitDownEdgeIndexFullScanRule>(new PushLimitDownEdgeIndexFullScanRule());
PushLimitDownEdgeIndexFullScanRule::PushLimitDownEdgeIndexFullScanRule() {
  RuleSet::QueryRules().addRule(this);
}
const Pattern &PushLimitDownEdgeIndexFullScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kEdgeIndexFullScan)});
  return pattern;
}
std::string PushLimitDownEdgeIndexFullScanRule::toString() const {
  return "PushLimitDownEdgeIndexFullScanRule";
}

std::unique_ptr<OptRule> PushLimitDownEdgeIndexPrefixScanRule::kInstance =
    std::unique_ptr<PushLimitDownEdgeIndexPrefixScanRule>(
        new PushLimitDownEdgeIndexPrefixScanRule());
PushLimitDownEdgeIndexPrefixScanRule::PushLimitDownEdgeIndexPrefixScanRule() {
  RuleSet::QueryRules().addRule(this);
}
const Pattern &PushLimitDownEdgeIndexPrefixScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kLimit,
                      {Pattern::create(graph::PlanNode::Kind::kEdgeIndexPrefixScan)});
  return pattern;
}
std::string PushLimitDownEdgeIndexPrefixScanRule::toString() const {
  return "PushLimitDownEdgeIndexPrefixScanRule";
}

std::unique_ptr<OptRule> PushLimitDownEdgeIndexRangeScanRule::kInstance =
    std::unique_ptr<PushLimitDownEdgeIndexRangeScanRule>(new PushLimitDownEdgeIndexRangeScanRule());
PushLimitDownEdgeIndexRangeScanRule::PushLimitDownEdgeIndexRangeScanRule() {
  RuleSet::QueryRules().addRule(this);
}
const Pattern &PushLimitDownEdgeIndexRangeScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kEdgeIndexRangeScan)});
  return pattern;
}
std::string PushLimitDownEdgeIndexRangeScanRule::toString() const {
  return "PushLimitDownEdgeIndexRangeScanRule";
}

std::unique_ptr<OptRule> PushLimitDownIndexScanRule::kInstance =
    std::unique_ptr<PushLimitDownIndexScanRule>(new PushLimitDownIndexScanRule());
PushLimitDownIndexScanRule::PushLimitDownIndexScanRule() { RuleSet::QueryRules().addRule(this); }
const Pattern &PushLimitDownIndexScanRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kIndexScan)});
  return pattern;
}
std::string PushLimitDownIndexScanRule::toString() const { return "PushLimitDownIndexScanRule"; }
}  // namespace opt
}  // namespace nebula
