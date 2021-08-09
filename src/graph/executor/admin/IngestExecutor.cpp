/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/IngestExecutor.h"
#include "planner/plan/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> IngestExecutor::execute() {
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->ingest(spaceId)
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Ingest failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
