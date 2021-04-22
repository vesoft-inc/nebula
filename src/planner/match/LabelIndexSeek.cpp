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
        VLOG(2) << "Multple tag index seek is not supported now.";
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

bool LabelIndexSeek::matchEdge(EdgeContext* edgeCtx) {
    const auto &edge = *edgeCtx->info;
    // require one edge at least
    if (edge.edgeTypes.size() != 1 || edge.range != nullptr) {
        // TODO multiple edge index seek need the IndexScan support
        VLOG(2) << "Multiple edge index seek and variable length edge seek are not supported now.";
        return false;
    }

    edgeCtx->scanInfo.schemaIds = edge.edgeTypes;
    edgeCtx->scanInfo.schemaNames = edge.types;

    auto indexResult = pickEdgeIndex(edgeCtx);
    if (!indexResult.ok()) {
        LOG(ERROR) << indexResult.status();
        return false;
    }

    edgeCtx->scanInfo.indexIds = std::move(indexResult).value();
    edgeCtx->scanInfo.direction = edge.direction;

    return true;
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

StatusOr<SubPlan> LabelIndexSeek::transformEdge(EdgeContext* edgeCtx) {
    SubPlan plan;
    auto* matchClauseCtx = edgeCtx->matchClauseCtx;
    DCHECK_EQ(edgeCtx->scanInfo.indexIds.size(), 1) << "Not supported multiple edge indices seek.";
    using IQC = nebula::storage::cpp2::IndexQueryContext;
    IQC iqctx;
    iqctx.set_index_id(edgeCtx->scanInfo.indexIds.back());
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back(std::move(iqctx));
    auto columns = std::make_unique<std::vector<std::string>>();
    std::vector<std::string> columnsName;
    switch (edgeCtx->scanInfo.direction) {
        case MatchEdge::Direction::OUT_EDGE:
            columns->emplace_back(kSrc);
            columnsName.emplace_back(kVid);
            break;
        case MatchEdge::Direction::IN_EDGE:
            columns->emplace_back(kDst);
            columnsName.emplace_back(kVid);
            break;
        case MatchEdge::Direction::BOTH:
            columns->emplace_back(kSrc);
            columns->emplace_back(kDst);
            columnsName.emplace_back(kSrc);
            columnsName.emplace_back(kDst);
            break;
    }
    auto scan = IndexScan::make(matchClauseCtx->qctx,
                                nullptr,
                                matchClauseCtx->space.id,
                                std::move(contexts),
                                std::move(columns),
                                true,
                                edgeCtx->scanInfo.schemaIds.back());
    scan->setColNames(columnsName);
    plan.tail = scan;
    plan.root = scan;

    if (edgeCtx->scanInfo.direction == MatchEdge::Direction::BOTH) {
        // merge the src,dst to one column
        auto *yieldColumns = matchClauseCtx->qctx->objPool()->makeAndAdd<YieldColumns>();
        auto *exprList = new ExpressionList();
        exprList->add(new ColumnExpression(0));  // src
        exprList->add(new ColumnExpression(1));  // dst
        yieldColumns->addColumn(new YieldColumn(new ListExpression(exprList)));
        auto *project = Project::make(matchClauseCtx->qctx,
                                      scan,
                                      yieldColumns);
        project->setColNames({kVid});
        auto *unwindColumns = matchClauseCtx->qctx->objPool()->makeAndAdd<YieldColumns>();
        unwindColumns->addColumn(new YieldColumn(new ColumnExpression(0)));
        auto *unwind = Unwind::make(matchClauseCtx->qctx, project, unwindColumns);
        unwind->setColNames({kVid});
        plan.root = unwind;
    }

    // initialize start expression in project node
    edgeCtx->initialExpr.reset(ExpressionUtils::newVarPropExpr(kVid));
    return plan;
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

/*static*/ StatusOr<std::vector<IndexID>>
LabelIndexSeek::pickEdgeIndex(const EdgeContext* edgeCtx) {
    std::vector<IndexID> indexIds;
    const auto* qctx = edgeCtx->matchClauseCtx->qctx;
    auto edgeIndexesResult = qctx->indexMng()->getEdgeIndexes(edgeCtx->matchClauseCtx->space.id);
    NG_RETURN_IF_ERROR(edgeIndexesResult);
    auto edgeIndexes = std::move(edgeIndexesResult).value();
    indexIds.reserve(edgeCtx->scanInfo.schemaIds.size());
    for (std::size_t i = 0; i < edgeCtx->scanInfo.schemaIds.size(); ++i) {
        auto edgeId = edgeCtx->scanInfo.schemaIds[i];
        std::shared_ptr<meta::cpp2::IndexItem> candidateIndex{nullptr};
        for (const auto& index : edgeIndexes) {
            if (index->get_schema_id().get_edge_type() == edgeId) {
                if (candidateIndex == nullptr) {
                    candidateIndex = index;
                } else {
                    candidateIndex = selectIndex(candidateIndex, index);
                }
            }
        }
        if (candidateIndex == nullptr) {
            return Status::SemanticError("No valid index for label `%s'.",
                                         edgeCtx->scanInfo.schemaNames[i]->c_str());
        }
        indexIds.emplace_back(candidateIndex->get_index_id());
    }
    return indexIds;
}

}   // namespace graph
}   // namespace nebula
