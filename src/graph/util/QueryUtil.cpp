/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/QueryUtil.h"
#include "common/base/Base.h"
#include "common/expression/ColumnExpression.h"
#include "context/QueryContext.h"
#include "context/ast/QueryAstContext.h"
#include "planner/plan/Query.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {

// static
void QueryUtil::buildConstantInput(QueryContext *qctx, Starts& starts, std::string& vidsVar) {
    vidsVar = qctx->vctx()->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : starts.vids) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    qctx->ectx()->setResult(vidsVar, ResultBuilder().value(Value(std::move(ds))).finish());
    auto* pool = qctx->objPool();
    // If possible, use column numbers in preference to column names,
    starts.src = ColumnExpression::make(pool, 0);
}

// static
SubPlan QueryUtil::buildRuntimeInput(QueryContext* qctx, Starts& starts) {
    auto pool = qctx->objPool();
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(starts.originalSrc->clone(), kVid);
    columns->addColumn(column);

    auto* project = Project::make(qctx, nullptr, columns);
    if (starts.fromType == kVariable) {
        project->setInputVar(starts.userDefinedVarName);
    }
    starts.src = InputPropertyExpression::make(pool, kVid);

    auto* dedup = Dedup::make(qctx, project);

    SubPlan subPlan;
    subPlan.root = dedup;
    subPlan.tail = project;
    return subPlan;
}

// static
SubPlan QueryUtil::buildStart(QueryContext* qctx, Starts& starts, std::string& vidsVar) {
    SubPlan subPlan;
    if (!starts.vids.empty() && starts.originalSrc == nullptr) {
        buildConstantInput(qctx, starts, vidsVar);
    } else {
        subPlan = buildRuntimeInput(qctx, starts);
        vidsVar = subPlan.root->outputVar();
    }
    return subPlan;
}

PlanNode* QueryUtil::extractDstFromGN(QueryContext* qctx, PlanNode* gn, const std::string& output) {
    auto pool = qctx->objPool();
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(EdgePropertyExpression::make(pool, "*", kDst), kVid);
    columns->addColumn(column);

    auto* project = Project::make(qctx, gn, columns);

    auto* dedup = Dedup::make(qctx, project);
    dedup->setOutputVar(output);
    return dedup;
}
}  // namespace graph
}  // namespace nebula
