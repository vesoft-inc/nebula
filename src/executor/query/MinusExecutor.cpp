/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/MinusExecutor.h"

#include <unordered_set>

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> MinusExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    NG_RETURN_IF_ERROR(checkInputDataSets());

    auto lIter = getLeftInputDataIter();
    auto rIter = getRightInputDataIter();

    std::unordered_set<const Row *> hashSet;
    for (; rIter->valid(); rIter->next()) {
        hashSet.insert(rIter->row());
        // TODO: should test duplicate rows
    }

    if (!hashSet.empty()) {
        while (lIter->valid()) {
            auto iter = hashSet.find(lIter->row());
            if (iter == hashSet.end()) {
                lIter->next();
            } else {
                lIter->unstableErase();
            }
        }
    }

    ResultBuilder builder;
    builder.value(lIter->valuePtr()).iter(std::move(lIter));
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
