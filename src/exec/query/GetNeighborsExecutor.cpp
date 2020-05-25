/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetNeighborsExecutor.h"

#include <sstream>

// common
#include "clients/storage/GraphStorageClient.h"
#include "datatypes/List.h"
#include "datatypes/Vertex.h"
// graph
#include "planner/Query.h"
#include "service/ExecutionContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetNeighborsExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));

        return getNeighbors().ensure([this]() {
            // TODO(yee): some cleanup or stats actions
            UNUSED(this);
        });
    }));
}

folly::Future<Status> GetNeighborsExecutor::getNeighbors() {
    const GetNeighbors* gn = asNode<GetNeighbors>(node());
    Expression* srcExpr = gn->src();
    Value value = srcExpr->eval();
    // TODO(yee): compute starting point
    UNUSED(value);

    std::vector<std::string> colNames;

    GraphStorageClient* storageClient = ectx()->getStorageClient();
    return storageClient
        ->getNeighbors(gn->space(),
                       std::move(colNames),
                       gn->vertices(),
                       gn->edgeTypes(),
                       gn->edgeDirection(),
                       &gn->statProps(),
                       &gn->vertexProps(),
                       &gn->edgeProps(),
                       gn->dedup(),
                       gn->orderBy(),
                       gn->limit(),
                       gn->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            auto completeness = resp.completeness();
            if (completeness != 0) {
                return error(Status::Error("Get neighbors failed"));
            }
            if (completeness != 100) {
                // TODO(dutor) We ought to let the user know that the execution was partially
                // performed, even in the case that this happened in the intermediate process.
                // Or, make this case configurable at runtime.
                // For now, we just do some logging and keep going.
                LOG(INFO) << "Get neighbors partially failed: " << completeness << "%";
                for (auto& error : resp.failedParts()) {
                    LOG(ERROR) << "part: " << error.first
                               << "error code: " << static_cast<int>(error.second);
                }
            }

            auto status = handleResponse(resp.responses());
            return status.ok() ? start() : error(std::move(status));
        });
}

Status GetNeighborsExecutor::handleResponse(const std::vector<GetNeighborsResponse>& responses) {
    for (auto& resp : responses) {
        checkResponseResult(resp.get_result());

        auto dataset = resp.get_vertices();
        if (dataset == nullptr) {
            LOG(INFO) << "Empty dataset in response";
            continue;
        }

        // Store response results to ExecutionContext
        return finish({*dataset});
    }
    return Status::Error("Invalid result of neighbors");
}

void GetNeighborsExecutor::checkResponseResult(const storage::cpp2::ResponseCommon& result) const {
    auto failedParts = result.get_failed_parts();
    if (!failedParts.empty()) {
        std::stringstream ss;
        for (auto& part : failedParts) {
            ss << "error code: " << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(part.get_code())
               << ", leader: " << part.get_leader()->host << ":" << part.get_leader()->port
               << ", part id: " << part.get_part_id() << "; ";
        }
        LOG(ERROR) << ss.str();
    }
}

}   // namespace graph
}   // namespace nebula
