/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/UnionAllIndexScanBaseRule.h"

#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptRule.h"
#include "optimizer/OptimizerUtils.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "planner/plan/Scan.h"

using nebula::graph::Filter;
using nebula::graph::IndexScan;
using nebula::graph::OptimizerUtils;
using nebula::graph::TagIndexFullScan;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

bool UnionAllIndexScanBaseRule::match(OptContext* ctx, const MatchedResult& matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto scan = static_cast<const IndexScan*>(matched.planNode({0, 0}));
    auto condition = filter->condition();
    if (!condition->isLogicalExpr() || condition->kind() != Expression::Kind::kLogicalOr) {
        return false;
    }

    for (auto operand : static_cast<const LogicalExpression*>(condition)->operands()) {
        if (!operand->isRelExpr()) {
            return false;
        }
    }

    for (auto& ictx : scan->queryContext()) {
        if (ictx.column_hints_ref().is_set()) {
            return false;
        }
    }

    return true;
}

StatusOr<TransformResult> UnionAllIndexScanBaseRule::transform(OptContext* ctx,
                                                               const MatchedResult& matched) const {
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto node = matched.planNode({0, 0});
    auto scan = static_cast<const IndexScan*>(node);

    auto metaClient = ctx->qctx()->getMetaClient();
    StatusOr<std::vector<std::shared_ptr<meta::cpp2::IndexItem>>> status;
    if (node->kind() == graph::PlanNode::Kind::kTagIndexFullScan) {
        status = metaClient->getTagIndexesFromCache(scan->space());
    } else {
        status = metaClient->getEdgeIndexesFromCache(scan->space());
    }
    NG_RETURN_IF_ERROR(status);
    auto indexItems = std::move(status).value();

    OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

    std::vector<IndexQueryContext> idxCtxs;
    auto condition = static_cast<const LogicalExpression*>(filter->condition());
    for (auto operand : condition->operands()) {
        IndexQueryContext ictx;
        bool isPrefixScan = false;
        if (!OptimizerUtils::findOptimalIndex(operand, indexItems, &isPrefixScan, &ictx)) {
            return TransformResult::noTransform();
        }
        idxCtxs.emplace_back(std::move(ictx));
    }

    auto scanNode = IndexScan::make(ctx->qctx(), nullptr);
    OptimizerUtils::copyIndexScanData(scan, scanNode);
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

}   // namespace opt
}   // namespace nebula
