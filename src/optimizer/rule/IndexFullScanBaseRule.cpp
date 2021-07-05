/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/IndexFullScanBaseRule.h"

#include "common/interface/gen-cpp2/storage_types.h"
#include "context/QueryContext.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptRule.h"
#include "optimizer/OptimizerUtils.h"
#include "planner/plan/Query.h"
#include "planner/plan/Scan.h"

using nebula::graph::IndexScan;
using nebula::graph::OptimizerUtils;
using nebula::storage::cpp2::IndexQueryContext;

using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

bool IndexFullScanBaseRule::match(OptContext* ctx, const MatchedResult& matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    auto scan = static_cast<const IndexScan*>(matched.planNode());
    for (auto& ictx : scan->queryContext()) {
        if (ictx.index_id_ref().is_set()) {
            return false;
        }
    }
    return true;
}

StatusOr<TransformResult> IndexFullScanBaseRule::transform(OptContext* ctx,
                                                           const MatchedResult& matched) const {
    auto scan = static_cast<const IndexScan*>(matched.planNode());

    auto metaClient = ctx->qctx()->getMetaClient();
    auto status = scan->isEdge() ? metaClient->getEdgeIndexesFromCache(scan->space())
                                 : metaClient->getTagIndexesFromCache(scan->space());
    NG_RETURN_IF_ERROR(status);
    auto indexItems = std::move(status).value();

    OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

    if (indexItems.empty()) {
        return TransformResult::noTransform();
    }

    std::vector<IndexQueryContext> idxCtxs;
    IndexQueryContext ictx;
    auto idxId = indexItems[0]->get_index_id();
    auto numFields = indexItems[0]->get_fields().size();
    for (size_t i = 1; i < indexItems.size(); ++i) {
        const auto& index = indexItems[i];
        if (numFields > index->get_fields().size()) {
            idxId = index->get_index_id();
        }
    }
    ictx.set_index_id(idxId);
    idxCtxs.emplace_back(std::move(ictx));

    auto scanNode = this->scan(ctx, scan);
    OptimizerUtils::copyIndexScanData(scan, scanNode);
    scanNode->setOutputVar(scan->outputVar());
    scanNode->setColNames(scan->colNames());
    scanNode->setIndexQueryContext(std::move(idxCtxs));
    auto filterGroup = matched.node->group();
    auto optScanNode = OptGroupNode::create(ctx, scanNode, filterGroup);
    for (auto group : matched.node->dependencies()) {
        optScanNode->dependsOn(group);
    }
    TransformResult result;
    result.newGroupNodes.emplace_back(optScanNode);
    result.eraseCurr = true;
    return result;
}

}   // namespace opt
}   // namespace nebula
