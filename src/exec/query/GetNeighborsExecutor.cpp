/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetNeighborsExecutor.h"

#include <sstream>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> GetNeighborsExecutor::execute() {
    auto status = buildRequestDataSet();
    if (!status.ok()) {
        return error(std::move(status));
    }
    return getNeighbors().ensure([this]() {
        // clear the members
        reqDs_.rows.clear();
    });
}

Status GetNeighborsExecutor::buildRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto& inputVar = gn_->inputVar();
    VLOG(1) << node()->varName() << " : " << inputVar;
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    QueryExpressionContext ctx(ectx_, iter.get());
    DataSet input;
    reqDs_.colNames = {kVid};
    reqDs_.rows.reserve(iter->size());
    auto* src = gn_->src();
    std::unordered_set<Value> uniqueVid;
    for (; iter->valid(); iter->next()) {
        auto val = Expression::eval(src, ctx);
        if (!val.isStr()) {
            continue;
        }
        auto ret = uniqueVid.emplace(val);
        if (ret.second) {
            reqDs_.rows.emplace_back(Row({std::move(val)}));
        }
    }

    return Status::OK();
}

folly::Future<Status> GetNeighborsExecutor::getNeighbors() {
    if (reqDs_.rows.empty()) {
        LOG(INFO) << "Empty input.";
        DataSet emptyResult;
        return finish(ResultBuilder()
                          .value(Value(std::move(emptyResult)))
                          .iter(Iterator::Kind::kGetNeighbors)
                          .finish());
    }

    time::Duration getNbrTime;
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    return storageClient
        ->getNeighbors(gn_->space(),
                       std::move(reqDs_.colNames),
                       std::move(reqDs_.rows),
                       gn_->edgeTypes(),
                       gn_->edgeDirection(),
                       gn_->statProps(),
                       gn_->vertexProps(),
                       gn_->edgeProps(),
                       gn_->exprs(),
                       gn_->dedup(),
                       gn_->random(),
                       gn_->orderBy(),
                       gn_->limit(),
                       gn_->filter())
        .via(runner())
        .ensure([getNbrTime]() {
            VLOG(1) << "Get neighbors time: " << getNbrTime.elapsedInUSec() << "us";
        })
        .then([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            SCOPED_TIMER(&execTime_);
            return handleResponse(resp);
        });
}

Status GetNeighborsExecutor::handleResponse(RpcResponse& resps) {
    auto completeness = resps.completeness();
    if (UNLIKELY(completeness == 0)) {
        return Status::Error("Get neighbors failed");
    }

    ResultBuilder builder;
    if (UNLIKELY(completeness != 100)) {
        builder.state(Result::State::kPartialSuccess)
            .msg(folly::stringPrintf("Get neighbors partially failed: %d %%", completeness));
    }

    auto& responses = resps.responses();
    VLOG(1) << "Resp size: " << responses.size();
    List list;
    for (auto& resp : responses) {
        checkResponseResult(resp.get_result());

        auto dataset = resp.get_vertices();
        if (dataset == nullptr) {
            LOG(INFO) << "Empty dataset in response";
            continue;
        }

        VLOG(1) << "Resp row size: " << dataset->rows.size() << "Resp : " << *dataset;
        list.values.emplace_back(std::move(*dataset));
    }
    builder.value(Value(std::move(list)));
    return finish(builder.iter(Iterator::Kind::kGetNeighbors).finish());
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
