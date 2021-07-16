/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/OptimizeEdgeIndexScanByFilterRule.h"
#include <algorithm>
#include <memory>
#include <vector>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "context/QueryContext.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptimizerUtils.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Scan.h"

using nebula::Expression;
using nebula::graph::EdgeIndexFullScan;
using nebula::graph::EdgeIndexPrefixScan;
using nebula::graph::EdgeIndexRangeScan;
using nebula::graph::EdgeIndexScan;
using nebula::graph::Filter;
using nebula::graph::OptimizerUtils;
using nebula::graph::QueryContext;
using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> OptimizeEdgeIndexScanByFilterRule::kInstance =
    std::unique_ptr<OptimizeEdgeIndexScanByFilterRule>(new OptimizeEdgeIndexScanByFilterRule());

OptimizeEdgeIndexScanByFilterRule::OptimizeEdgeIndexScanByFilterRule() {
    RuleSet::DefaultRules().addRule(this);
}

const Pattern& OptimizeEdgeIndexScanByFilterRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kEdgeIndexFullScan)});
    return pattern;
}

bool OptimizeEdgeIndexScanByFilterRule::match(OptContext* ctx, const MatchedResult& matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto scan = static_cast<const EdgeIndexFullScan*>(matched.planNode({0, 0}));
    for (auto& ictx : scan->queryContext()) {
        if (ictx.column_hints_ref().is_set()) {
            return false;
        }
    }
    auto condition = filter->condition();
    if (condition->isRelExpr()) {
        auto relExpr = static_cast<const RelationalExpression*>(condition);
        return relExpr->left()->kind() == ExprKind::kEdgeProperty &&
               relExpr->right()->kind() == ExprKind::kConstant;
    }
    if (condition->isLogicalExpr()) {
        return condition->kind() == Expression::Kind::kLogicalAnd;
    }

    return false;
}

EdgeIndexScan* makeEdgeIndexScan(QueryContext* qctx, const EdgeIndexScan* scan, bool isPrefixScan) {
    EdgeIndexScan* scanNode = nullptr;
    if (isPrefixScan) {
        scanNode = EdgeIndexPrefixScan::make(qctx, nullptr, scan->edgeType());
    } else {
        scanNode = EdgeIndexRangeScan::make(qctx, nullptr, scan->edgeType());
    }
    OptimizerUtils::copyIndexScanData(scan, scanNode);
    return scanNode;
}

StatusOr<TransformResult> OptimizeEdgeIndexScanByFilterRule::transform(
    OptContext* ctx,
    const MatchedResult& matched) const {
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto scan = static_cast<const EdgeIndexFullScan*>(matched.planNode({0, 0}));

    auto metaClient = ctx->qctx()->getMetaClient();
    auto status = metaClient->getEdgeIndexesFromCache(scan->space());
    NG_RETURN_IF_ERROR(status);
    auto indexItems = std::move(status).value();

    OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

    IndexQueryContext ictx;
    bool isPrefixScan = false;
    if (!OptimizerUtils::findOptimalIndex(filter->condition(), indexItems, &isPrefixScan, &ictx)) {
        return TransformResult::noTransform();
    }
    std::vector<IndexQueryContext> idxCtxs = {ictx};
    EdgeIndexScan* scanNode = makeEdgeIndexScan(ctx->qctx(), scan, isPrefixScan);
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

std::string OptimizeEdgeIndexScanByFilterRule::toString() const {
    return "OptimizeEdgeIndexScanByFilterRule";
}

}   // namespace opt
}   // namespace nebula
