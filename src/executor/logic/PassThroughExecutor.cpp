/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/logic/PassThroughExecutor.h"

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> PassThroughExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    DataSet ds;
    ds.colNames = node()->colNames();
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
