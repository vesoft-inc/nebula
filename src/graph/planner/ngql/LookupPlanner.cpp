/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ngql/LookupPlanner.h"

#include <algorithm>
#include <tuple>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/expression/Expression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/PropertyExpression.h"
#include "context/ast/QueryAstContext.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "planner/Planner.h"
#include "planner/plan/Scan.h"
#include "visitor/FindVisitor.h"

namespace nebula {
namespace graph {

static std::tuple<const char*, const char*> kEdgeKeys[3] = {
    {kSrcVID, kSrc},
    {kDstVID, kDst},
    {kRanking, kRank},
};

std::unique_ptr<Planner> LookupPlanner::make() {
    return std::unique_ptr<LookupPlanner>(new LookupPlanner());
}

bool LookupPlanner::match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kLookup;
}

StatusOr<SubPlan> LookupPlanner::transform(AstContext* astCtx) {
    auto lookupCtx = static_cast<LookupContext*>(astCtx);
    auto yieldCols = prepareReturnCols(lookupCtx);
    auto qctx = lookupCtx->qctx;
    auto from = static_cast<const LookupSentence*>(lookupCtx->sentence)->from();
    SubPlan plan;
    if (lookupCtx->isEdge) {
        plan.tail = EdgeIndexFullScan::make(qctx,
                                            nullptr,
                                            from,
                                            lookupCtx->space.id,
                                            {},
                                            returnCols_,
                                            lookupCtx->schemaId,
                                            lookupCtx->isEmptyResultSet);
    } else {
        plan.tail = TagIndexFullScan::make(qctx,
                                           nullptr,
                                           from,
                                           lookupCtx->space.id,
                                           {},
                                           returnCols_,
                                           lookupCtx->schemaId,
                                           lookupCtx->isEmptyResultSet);
    }
    plan.tail->setColNames(colNames_);

    plan.root = plan.tail;

    if (lookupCtx->filter) {
        plan.root = Filter::make(qctx, plan.root, lookupCtx->filter);
    }

    plan.root = Project::make(qctx, plan.root, yieldCols);
    return plan;
}

YieldColumns* LookupPlanner::prepareReturnCols(LookupContext* lookupCtx) {
    auto pool = lookupCtx->qctx->objPool();
    auto columns = pool->makeAndAdd<YieldColumns>();
    auto addColumn = [this, pool, columns](const auto& tup) {
        std::string name(std::get<0>(tup));
        auto expr = InputPropertyExpression::make(pool, name);
        columns->addColumn(new YieldColumn(expr, name));
        addLookupColumns(std::get<1>(tup), name);
    };
    if (lookupCtx->isEdge) {
        for (auto& key : kEdgeKeys) {
            addColumn(key);
        }
    } else {
        addColumn(std::make_tuple(kVertexID, kVid));
    }
    extractUsedColumns(lookupCtx->filter);
    appendColumns(lookupCtx, columns);
    return columns;
}

void LookupPlanner::appendColumns(LookupContext* lookupCtx, YieldColumns* columns) {
    auto sentence = static_cast<LookupSentence*>(lookupCtx->sentence);
    auto yieldClause = sentence->yieldClause();
    if (yieldClause == nullptr) return;
    auto pool = lookupCtx->qctx->objPool();
    for (auto col : yieldClause->columns()) {
        auto expr = col->expr();
        DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
        auto laExpr = static_cast<LabelAttributeExpression*>(expr);
        const auto& schemaName = laExpr->left()->name();
        const auto& colName = laExpr->right()->value().getStr();
        Expression* propExpr = nullptr;
        if (lookupCtx->isEdge) {
            propExpr = EdgePropertyExpression::make(pool, schemaName, colName);
        } else {
            propExpr = TagPropertyExpression::make(pool, schemaName, colName);
        }
        columns->addColumn(new YieldColumn(propExpr, col->alias()));
        addLookupColumns(colName, propExpr->toString());
    }
}

void LookupPlanner::extractUsedColumns(Expression* filter) {
    if (filter == nullptr) return;

    auto finder = [](Expression* expr) {
        return expr->kind() == Expression::Kind::kTagProperty ||
               expr->kind() == Expression::Kind::kEdgeProperty;
    };
    FindVisitor visitor(finder, true);
    filter->accept(&visitor);
    for (auto expr : std::move(visitor).results()) {
        auto propExpr = static_cast<const PropertyExpression*>(expr);
        addLookupColumns(propExpr->prop(), propExpr->toString());
    }
}

void LookupPlanner::addLookupColumns(const std::string& retCol, const std::string& outCol) {
    auto iter = std::find(returnCols_.begin(), returnCols_.end(), retCol);
    if (iter == returnCols_.end()) {
        returnCols_.emplace_back(retCol);
    }
    iter = std::find(colNames_.begin(), colNames_.end(), outCol);
    if (iter == colNames_.end()) {
        colNames_.emplace_back(outCol);
    }
}

}   // namespace graph
}   // namespace nebula
