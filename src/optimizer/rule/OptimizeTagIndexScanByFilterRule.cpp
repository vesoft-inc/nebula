/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/OptimizeTagIndexScanByFilterRule.h"

#include "common/expression/Expression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "context/QueryContext.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptimizerUtils.h"
#include "optimizer/rule/IndexScanRule.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Scan.h"

using nebula::graph::Filter;
using nebula::graph::OptimizerUtils;
using nebula::graph::QueryContext;
using nebula::graph::TagIndexFullScan;
using nebula::graph::TagIndexPrefixScan;
using nebula::graph::TagIndexRangeScan;
using nebula::graph::TagIndexScan;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> OptimizeTagIndexScanByFilterRule::kInstance =
    std::unique_ptr<OptimizeTagIndexScanByFilterRule>(new OptimizeTagIndexScanByFilterRule());

OptimizeTagIndexScanByFilterRule::OptimizeTagIndexScanByFilterRule() {
    RuleSet::DefaultRules().addRule(this);
}

const Pattern& OptimizeTagIndexScanByFilterRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kTagIndexFullScan)});
    return pattern;
}

bool OptimizeTagIndexScanByFilterRule::match(OptContext* ctx, const MatchedResult& matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto scan = static_cast<const TagIndexFullScan*>(matched.planNode({0, 0}));
    for (auto& ictx : scan->queryContext()) {
        if (ictx.column_hints_ref().is_set()) {
            return false;
        }
    }
    auto condition = filter->condition();
    if (condition->isRelExpr()) {
        auto relExpr = static_cast<const RelationalExpression*>(condition);
        return relExpr->left()->kind() == ExprKind::kTagProperty &&
               relExpr->right()->kind() == ExprKind::kConstant;
    }
    if (condition->isLogicalExpr()) {
        return condition->kind() == Expression::Kind::kLogicalAnd;
    }

    return false;
}

TagIndexScan* makeTagIndexScan(QueryContext* qctx, const TagIndexScan* scan, bool isPrefixScan) {
    TagIndexScan* tagScan = nullptr;
    if (isPrefixScan) {
        tagScan = TagIndexPrefixScan::make(qctx, nullptr, scan->tagName());
    } else {
        tagScan = TagIndexRangeScan::make(qctx, nullptr, scan->tagName());
    }

    OptimizerUtils::copyIndexScanData(scan, tagScan);
    return tagScan;
}

StatusOr<TransformResult> OptimizeTagIndexScanByFilterRule::transform(
    OptContext* ctx,
    const MatchedResult& matched) const {
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto scan = static_cast<const TagIndexFullScan*>(matched.planNode({0, 0}));

    auto metaClient = ctx->qctx()->getMetaClient();
    auto status = metaClient->getTagIndexesFromCache(scan->space());
    NG_RETURN_IF_ERROR(status);
    auto indexItems = std::move(status).value();

    OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

    IndexQueryContext ictx;
    bool isPrefixScan = false;
    if (!OptimizerUtils::findOptimalIndex(filter->condition(), indexItems, &isPrefixScan, &ictx)) {
        return TransformResult::noTransform();
    }

    std::vector<IndexQueryContext> idxCtxs = {ictx};
    auto scanNode = makeTagIndexScan(ctx->qctx(), scan, isPrefixScan);
    scanNode->setIndexQueryContext(std::move(idxCtxs));
    scanNode->setOutputVar(filter->outputVar());
    scanNode->setColNames(filter->colNames());
    auto filterGroup = matched.node->group();
    auto optScanNode = OptGroupNode::create(ctx, scanNode, filterGroup);
    for (auto group : matched.dependencies[0].node->dependencies()) {
        optScanNode->dependsOn(group);
    }
    TransformResult result;
    result.newGroupNodes.emplace_back(optScanNode);
    result.eraseCurr = true;
    return result;
}

std::string OptimizeTagIndexScanByFilterRule::toString() const {
    return "OptimizeTagIndexScanByFilterRule";
}

}   // namespace opt
}   // namespace nebula
