/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/mutate/InsertEdgesExecutor.h"

#include "common/clients/storage/GraphStorageClient.h"

#include "planner/Mutate.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"


namespace nebula {
namespace graph {

folly::Future<Status> InsertEdgesExecutor::execute() {
    return insertEdges().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> InsertEdgesExecutor::insertEdges() {
    SCOPED_TIMER(&execTime_);

    auto *ieNode = asNode<InsertEdges>(node());
    time::Duration addEdgeTime;
    return qctx()
        ->getStorageClient()
        ->addEdges(ieNode->getSpace(),
                   ieNode->getEdges(),
                   ieNode->getPropNames(),
                   ieNode->getOverwritable())
        .via(runner())
        .ensure([addEdgeTime]() {
            VLOG(1) << "Add edge time: " << addEdgeTime.elapsedInUSec() << "us";
        })
        .then([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
            SCOPED_TIMER(&execTime_);
            auto completeness = resp.completeness();
            if (completeness != 100) {
                const auto& failedCodes = resp.failedParts();
                for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                    LOG(ERROR) << "Insert edges failed, error "
                               << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)
                               << ", part " << it->first;
                }
                return Status::Error("Insert edges not complete, completeness: %d",
                                      completeness);
            }
            return finish(ResultBuilder().value(Value()).iter(Iterator::Kind::kDefault).finish());
        });
}

}   // namespace graph
}   // namespace nebula
