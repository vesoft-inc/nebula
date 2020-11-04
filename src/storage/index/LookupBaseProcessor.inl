/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "LookupBaseProcessor.h"
namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
cpp2::ErrorCode LookupBaseProcessor<REQ, RESP>::requestCheck(const cpp2::LookupIndexRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = this->getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    planContext_ = std::make_unique<PlanContext>(
        this->env_, spaceId_, this->spaceVidLen_, this->isIntId_);
    const auto& indices = req.get_indices();
    planContext_->isEdge_ = indices.get_is_edge();
    if (planContext_->isEdge_) {
        planContext_->edgeType_ = indices.get_tag_or_edge_id();
    } else {
        planContext_->tagId_ = indices.get_tag_or_edge_id();
    }

    if (indices.get_contexts().empty()) {
        return cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    contexts_ = indices.get_contexts();

    // setup yield columns.
    if (req.__isset.return_columns) {
        yieldCols_ = *req.get_return_columns();
    }

    for (const auto& ctx : contexts_) {
        const auto& indexId = ctx.get_index_id();
        auto needFilter = ctx.__isset.filter && !ctx.get_filter().empty();

        auto index = planContext_->isEdge_
            ? this->env_->indexMan_->getEdgeIndex(spaceId_, indexId)
            : this->env_->indexMan_->getTagIndex(spaceId_, indexId);
        if (!index.ok()) {
            return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
        }

        auto fields = index.value()->get_fields();

        // If no filed in fields, only _vid can be returned for tag.
        // For edge, only _src, _ranking, _dst can be returned
        if (fields.size() == 0) {
            //
            if (contexts_.size() != 1 || yieldCols_.size() != 0 ||
                needFilter || ctx.get_column_hints().size() != 0) {
                return cpp2::ErrorCode::E_INVALID_OPERATION;
            }
            noProp_ = true;
        }
    }

    // setup result set columns.
    if (planContext_->isEdge_) {
        resultDataSet_.colNames.emplace_back(kSrc);
        resultDataSet_.colNames.emplace_back("_ranking");
        resultDataSet_.colNames.emplace_back(kDst);
    } else {
        resultDataSet_.colNames.emplace_back(kVid);
    }

    for (const auto& col : yieldCols_) {
        resultDataSet_.colNames.emplace_back(col);
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
bool LookupBaseProcessor<REQ, RESP>::isOutsideIndex(Expression* filter,
                                                    const meta::cpp2::IndexItem* index) {
    auto fields = index->get_fields();
    switch (filter->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto lExpr = static_cast<LogicalExpression*>(filter);
            auto ret = isOutsideIndex(lExpr->left(), index);
            if (ret) {
                return ret;
            }
            ret = isOutsideIndex(lExpr->right(), index);
            if (ret) {
                return ret;
            }
            break;
        }
        case Expression::Kind::kRelLE :
        case Expression::Kind::kRelIn :
        case Expression::Kind::kRelGE :
        case Expression::Kind::kRelEQ :
        case Expression::Kind::kRelLT :
        case Expression::Kind::kRelGT :
        case Expression::Kind::kRelNE :
        case Expression::Kind::kRelNotIn : {
            auto* rExpr = static_cast<RelationalExpression*>(filter);
            auto ret = isOutsideIndex(rExpr->left(), index);
            if (ret) {
                return ret;
            }
            ret = isOutsideIndex(rExpr->right(), index);
            if (ret) {
                return ret;
            }
            break;
        }
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kEdgeProperty: {
            auto* sExpr = static_cast<PropertyExpression*>(filter);
            auto propName = *(sExpr->prop());
            auto it = std::find_if(fields.begin(), fields.end(), [&propName] (const auto& f) {
                return f.get_name() == propName;
            });
            return it == fields.end();
        }
        default: {
            return false;
        }
    }
    return false;
}

/**
 * lookup plan should be :
 *              +--------+---------+
 *              |       Plan       |
 *              +--------+---------+
 *                       |
 *              +--------+---------+
 *              |  AggregateNode   |
 *              +--------+---------+
 *                       |
 *            +----------+-----------+
 *            +  IndexOutputNode...  +
 *            +----------+-----------+
**/

template<typename REQ, typename RESP>
StatusOr<StoragePlan<IndexID>> LookupBaseProcessor<REQ, RESP>::buildPlan() {
    StoragePlan<IndexID> plan;
    auto IndexAggr = std::make_unique<AggregateNode<IndexID>>(&resultDataSet_);
    int32_t filterId = 0;
    std::unique_ptr<IndexOutputNode<IndexID>> out;

    if (noProp_) {
        out = buildPlanBasic(contexts_[0], plan, false, {});
         if (out == nullptr) {
            return Status::Error("Index scan plan error");
        }
        IndexAggr->addDependency(out.get());
        plan.addNode(std::move(out));
    } else {
        for (const auto& ctx : contexts_) {
            const auto& indexId = ctx.get_index_id();
            auto needFilter = ctx.__isset.filter && !ctx.get_filter().empty();

            // Check whether a data node is required.
            // If a non-indexed column appears in the WHERE clause or YIELD clause,
            // That means need to query the corresponding data.
            bool needData = false;
            auto index = planContext_->isEdge_
                ? this->env_->indexMan_->getEdgeIndex(spaceId_, indexId)
                : this->env_->indexMan_->getTagIndex(spaceId_, indexId);
            if (!index.ok()) {
                return Status::IndexNotFound();
            }

            // check nullable column
            bool hasNullableCol = false;

            auto* indexItem = index.value().get();
            auto fields = indexItem->get_fields();

            for (const auto& col : fields) {
                if (!hasNullableCol && col.get_nullable()) {
                    hasNullableCol = true;
                    break;
                }
            }

            for (const auto& yieldCol : yieldCols_) {
                auto it = std::find_if(fields.begin(),
                                       fields.end(),
                                       [&yieldCol] (const auto& columnDef) {
                                           return yieldCol == columnDef.get_name();
                                       });
                if (it == fields.end()) {
                    needData = true;
                    break;
                }
            }
            auto colHints = ctx.get_column_hints();

            // Check WHERE clause contains columns that ware not indexed
            if (ctx.__isset.filter && !ctx.get_filter().empty()) {
                auto filter = Expression::decode(ctx.get_filter());
                auto isFieldsOutsideIndex = isOutsideIndex(filter.get(), indexItem);
                if (isFieldsOutsideIndex) {
                    needData = needFilter = true;
                }
            }

            if (!needData && !needFilter) {
                out = buildPlanBasic(ctx, plan, hasNullableCol, fields);
            } else if (needData && !needFilter) {
                out = buildPlanWithData(ctx, plan);
            } else if (!needData && needFilter) {
                auto expr = Expression::decode(ctx.get_filter());
                auto exprCtx = std::make_unique<StorageExpressionContext>(planContext_->vIdLen_,
                                                                          planContext_->isIntId_,
                                                                          hasNullableCol,
                                                                          fields);
                filterItems_.emplace(filterId, std::make_pair(std::move(exprCtx), std::move(expr)));
                out = buildPlanWithFilter(ctx,
                                          plan,
                                          filterItems_[filterId].first.get(),
                                          filterItems_[filterId].second.get());
                filterId++;
            } else {
                auto expr = Expression::decode(ctx.get_filter());
                auto exprCtx = std::make_unique<StorageExpressionContext>(planContext_->vIdLen_,
                                                                          planContext_->isIntId_);
                filterItems_.emplace(filterId, std::make_pair(std::move(exprCtx), std::move(expr)));
                out = buildPlanWithDataAndFilter(ctx,
                                                 plan,
                                                 filterItems_[filterId].first.get(),
                                                 filterItems_[filterId].second.get());
                filterId++;
            }
            if (out == nullptr) {
                return Status::Error("Index scan plan error");
            }
            IndexAggr->addDependency(out.get());
            plan.addNode(std::move(out));
        }
    }
    plan.addNode(std::move(IndexAggr));
    return plan;
}

/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If this is a simple index scan, Just having IndexScanNode is enough. for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2, c3)
 * hint : lookup index where c1 == 1 and c2 == 1 and c3 == 1 yield c1,c2,c3
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanBasic(
    const cpp2::IndexQueryContext& ctx,
    StoragePlan<IndexID>& plan,
    bool hasNullableCol,
    const std::vector<meta::cpp2::ColumnDef>& fields) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));

    auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                             planContext_.get(),
                                                             indexScan.get(),
                                                             hasNullableCol,
                                                             fields);
    output->addDependency(indexScan.get());
    plan.addNode(std::move(indexScan));
    return output;
}

/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *      +----------------+-----------------+
 *      + IndexEdgeNode or IndexVertexNode +
 *      +----------------+-----------------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If a non-indexed column appears in the YIELD clause, and no expression filtering is required .
 * for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2)
 * hint : lookup index where c1 == 1 and c2 == 1 yield c3
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanWithData(const cpp2::IndexQueryContext& ctx,
                                                  StoragePlan<IndexID>& plan) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schema = planContext_->isEdge_
                  ? this->env_->schemaMan_->getEdgeSchema(this->spaceId_, planContext_->edgeType_)
                  : this->env_->schemaMan_->getTagSchema(this->spaceId_, planContext_->tagId_);
    if (!schema) {
        return nullptr;
    }

    auto schemaName = planContext_->isEdge_
                      ? this->env_->schemaMan_->toEdgeName(spaceId_, planContext_->edgeType_)
                      : this->env_->schemaMan_->toTagName(spaceId_, planContext_->tagId_);
    if (!schemaName.ok()) {
        return nullptr;
    }

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));
    if (planContext_->isEdge_) {
        auto edge = std::make_unique<IndexEdgeNode<IndexID>>(planContext_.get(),
                                                             indexScan.get(),
                                                             std::move(schema),
                                                             std::move(schemaName).value());
        edge->addDependency(indexScan.get());
        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 planContext_.get(),
                                                                 edge.get());
        output->addDependency(edge.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(edge));
        return output;
    } else {
        auto vertex = std::make_unique<IndexVertexNode<IndexID>>(planContext_.get(),
                                                                 this->vertexCache_,
                                                                 indexScan.get(),
                                                                 std::move(schema),
                                                                 std::move(schemaName).value());
        vertex->addDependency(indexScan.get());
        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 planContext_.get(),
                                                                 vertex.get());
        output->addDependency(vertex.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(vertex));
        return output;
    }
}

/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +  IndexFilterNode     +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If have not non-indexed column appears in the YIELD clause, and expression filtering is required .
 * for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2)
 * hint : lookup index where c1 > 1 and c2 > 1
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanWithFilter(const cpp2::IndexQueryContext& ctx,
                                                    StoragePlan<IndexID>& plan,
                                                    StorageExpressionContext* exprCtx,
                                                    Expression* exp) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));

    auto filter = std::make_unique<IndexFilterNode<IndexID>>(indexScan.get(),
                                                             exprCtx,
                                                             exp,
                                                             planContext_->isEdge_);
    filter->addDependency(indexScan.get());
    auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                             planContext_.get(),
                                                             filter.get(), true);
    output->addDependency(filter.get());
    plan.addNode(std::move(indexScan));
    plan.addNode(std::move(filter));
    return output;
}


/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +   IndexFilterNode    +
 *            +----------+-----------+
 *                       |
 *      +----------------+-----------------+
 *      + IndexEdgeNode or IndexVertexNode +
 *      +----------------+-----------------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If a non-indexed column appears in the WHERE clause or YIELD clause,
 * and expression filtering is required .
 * for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2)
 * hint : lookup index where c1 == 1 and c2 == 1 and c3 > 1 yield c3
 *        lookup index where c1 == 1 and c2 == 1 and c3 > 1
 *        lookup index where c1 == 1 and c3 == 1
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanWithDataAndFilter(const cpp2::IndexQueryContext& ctx,
                                                           StoragePlan<IndexID>& plan,
                                                           StorageExpressionContext* exprCtx,
                                                           Expression* exp) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schema = planContext_->isEdge_
                  ? this->env_->schemaMan_->getEdgeSchema(spaceId_, planContext_->edgeType_)
                  : this->env_->schemaMan_->getTagSchema(spaceId_, planContext_->tagId_);
    if (!schema) {
        return nullptr;
    }

    auto schemaName = planContext_->isEdge_
                      ? this->env_->schemaMan_->toEdgeName(spaceId_, planContext_->edgeType_)
                      : this->env_->schemaMan_->toTagName(spaceId_, planContext_->tagId_);
    if (!schemaName.ok()) {
        return nullptr;
    }
    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));
    if (planContext_->isEdge_) {
        auto edge = std::make_unique<IndexEdgeNode<IndexID>>(planContext_.get(),
                                                             indexScan.get(),
                                                             std::move(schema),
                                                             std::move(schemaName).value());
        edge->addDependency(indexScan.get());
        auto filter = std::make_unique<IndexFilterNode<IndexID>>(edge.get(),
                                                                 exprCtx,
                                                                 exp);
        filter->addDependency(edge.get());

        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 planContext_.get(),
                                                                 filter.get());
        output->addDependency(filter.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(edge));
        plan.addNode(std::move(filter));
        return output;
    } else {
        auto vertex = std::make_unique<IndexVertexNode<IndexID>>(planContext_.get(),
                                                                 this->vertexCache_,
                                                                 indexScan.get(),
                                                                 std::move(schema),
                                                                 std::move(schemaName).value());
        vertex->addDependency(indexScan.get());
        auto filter = std::make_unique<IndexFilterNode<IndexID>>(vertex.get(),
                                                                 exprCtx,
                                                                 exp);
        filter->addDependency(vertex.get());

        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 planContext_.get(),
                                                                 filter.get());
        output->addDependency(filter.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(vertex));
        plan.addNode(std::move(filter));
        return output;
    }
}

}  // namespace storage
}  // namespace nebula
