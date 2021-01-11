/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/LabelIndexSeek.h"
#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {

bool LabelIndexSeek::matchNode(NodeContext* nodeCtx) {
    auto& node = *nodeCtx->info;
    // only require the tag
    if (node.tids.size() != 1) {
        // TODO multiple tag index seek need the IndexScan support
        return false;
    }

    nodeCtx->scanInfo.schemaIds = node.tids;
    nodeCtx->scanInfo.schemaNames = node.labels;

    auto indexResult = pickTagIndex(nodeCtx);
    if (!indexResult.ok()) {
        return false;
    }

    nodeCtx->scanInfo.indexIds = std::move(indexResult).value();

    return true;
}

bool LabelIndexSeek::matchEdge(EdgeContext*) {
    // TODO
    return false;
}

StatusOr<SubPlan> LabelIndexSeek::transformNode(NodeContext* nodeCtx) {
    SubPlan plan;
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    DCHECK_EQ(nodeCtx->scanInfo.indexIds.size(), 1) << "Not supported multiple tag index seek.";
    using IQC = nebula::storage::cpp2::IndexQueryContext;
    IQC iqctx;
    iqctx.set_index_id(nodeCtx->scanInfo.indexIds.back());
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back(std::move(iqctx));
    auto columns = std::make_unique<std::vector<std::string>>();
    columns->emplace_back(kVid);
    auto scan = IndexScan::make(matchClauseCtx->qctx,
                                nullptr,
                                matchClauseCtx->space.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                nodeCtx->scanInfo.schemaIds.back());
    scan->setColNames({kVid});
    plan.tail = scan;
    plan.root = scan;

    // initialize start expression in project node
    nodeCtx->initialExpr.reset(ExpressionUtils::newVarPropExpr(kVid));
    return plan;
}

StatusOr<SubPlan> LabelIndexSeek::transformEdge(EdgeContext*) {
    // TODO
    return Status::Error("TODO");
}

/*static*/ StatusOr<std::vector<IndexID>> LabelIndexSeek::pickTagIndex(const NodeContext* nodeCtx) {
    std::vector<IndexID> indexIds;
    const auto* qctx = nodeCtx->matchClauseCtx->qctx;
    auto tagIndexesResult = qctx->indexMng()->getTagIndexes(nodeCtx->matchClauseCtx->space.id);
    NG_RETURN_IF_ERROR(tagIndexesResult);
    auto tagIndexes = std::move(tagIndexesResult).value();
    indexIds.reserve(nodeCtx->scanInfo.schemaIds.size());
    for (std::size_t i = 0; i < nodeCtx->scanInfo.schemaIds.size(); ++i) {
        auto tagId = nodeCtx->scanInfo.schemaIds[i];
        std::shared_ptr<meta::cpp2::IndexItem> candidateIndex{nullptr};
        for (const auto& index : tagIndexes) {
            if (index->get_schema_id().get_tag_id() == tagId) {
                if (candidateIndex == nullptr) {
                    candidateIndex = index;
                } else {
                    candidateIndex = selectIndex(candidateIndex, index);
                }
            }
        }
        if (candidateIndex == nullptr) {
            return Status::SemanticError("No valid index for label `%s'.",
                                        nodeCtx->scanInfo.schemaNames[i]->c_str());
        }
        indexIds.emplace_back(candidateIndex->get_index_id());
    }
    return indexIds;
}

}   // namespace graph
}   // namespace nebula
