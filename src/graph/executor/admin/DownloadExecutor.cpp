/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/DownloadExecutor.h"
#include "planner/plan/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> DownloadExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dNode = asNode<Download>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->download(dNode->getHdfsHost(),
                                             dNode->getHdfsPort(),
                                             dNode->getHdfsPath(),
                                             spaceId)
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Download failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
