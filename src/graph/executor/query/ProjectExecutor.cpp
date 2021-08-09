/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/ProjectExecutor.h"

#include "context/QueryExpressionContext.h"
#include "parser/Clauses.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> ProjectExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* project = asNode<Project>(node());
    auto columns = project->columns()->columns();
    auto iter = ectx_->getResult(project->inputVar()).iter();
    DCHECK(!!iter);
    QueryExpressionContext ctx(ectx_);

    VLOG(1) << "input: " << project->inputVar();
    DataSet ds;
    ds.colNames = project->colNames();
    ds.rows.reserve(iter->size());
    for (; iter->valid(); iter->next()) {
        Row row;
        for (auto& col : columns) {
            Value val = col->expr()->eval(ctx(iter.get()));
            row.values.emplace_back(std::move(val));
        }
        ds.rows.emplace_back(std::move(row));
    }
    VLOG(1) << node()->outputVar() << ":" << ds;
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
