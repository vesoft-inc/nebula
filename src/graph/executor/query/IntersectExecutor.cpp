/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/IntersectExecutor.h"

#include <unordered_set>

#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> IntersectExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    NG_RETURN_IF_ERROR(checkInputDataSets());

    auto left = getLeftInputData();
    auto right = getRightInputData();

    std::unordered_set<const Row *> hashSet;
    for (; right.iterRef()->valid(); right.iterRef()->next()) {
        hashSet.insert(right.iterRef()->row());
        // TODO: should test duplicate rows
    }

    auto* lIter = left.iterRef();
    ResultBuilder builder;
    if (hashSet.empty()) {
        auto value = lIter->valuePtr();
        DataSet ds;
        ds.colNames = value->getDataSet().colNames;
        builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
        return finish(builder.finish());
    }

    while (lIter->valid()) {
        auto iter = hashSet.find(lIter->row());
        if (iter == hashSet.end()) {
            lIter->unstableErase();
        } else {
            lIter->next();
        }
    }

    builder.value(left.valuePtr()).iter(std::move(left).iter());
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
