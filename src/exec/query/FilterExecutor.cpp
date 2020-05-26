/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/FilterExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
    dumpLog();
    auto* filter = asNode<Filter>(node());
    auto* expr = filter->condition();
    UNUSED(expr);
    return start();
}

}   // namespace graph
}   // namespace nebula
