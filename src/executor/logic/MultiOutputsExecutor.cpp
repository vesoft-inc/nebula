/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/logic/MultiOutputsExecutor.h"

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> MultiOutputsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
