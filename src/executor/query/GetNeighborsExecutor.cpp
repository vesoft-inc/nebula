/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetNeighborsExecutor.h"

#include <sstream>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
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
    return getNeighbors();
}

Status GetNeighborsExecutor::close() {
    // clear the members
    reqDs_.rows.clear();
    return Executor::close();
}

Status GetNeighborsExecutor::buildRequestDataSet() {
    SCOPED_TIMER(&execTime_);
    auto inputVar = gn_->inputVar();
    VLOG(1) << node()->outputVar() << " : " << inputVar;
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    QueryExpressionContext ctx(ectx_);
    DataSet input;
    reqDs_.colNames = {kVid};
    reqDs_.rows.reserve(iter->size());
    auto* src = gn_->src();
    std::unordered_set<Value> uniqueVid;
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    for (; iter->valid(); iter->next()) {
        auto val = Expression::eval(src, ctx(iter.get()));
        if (!SchemaUtil::isValidVid(val, spaceInfo.spaceDesc.vid_type)) {
            continue;
        }
        if (gn_->dedup()) {
            auto ret = uniqueVid.emplace(val);
            if (ret.second) {
                reqDs_.rows.emplace_back(Row({std::move(val)}));
            }
        } else {
            reqDs_.rows.emplace_back(Row({std::move(val)}));
        }
    }
    return Status::OK();
}

folly::Future<Status> GetNeighborsExecutor::getNeighbors() {
    if (reqDs_.rows.empty()) {
        VLOG(1) << "Empty input.";
        List emptyResult;
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
    auto result = handleCompleteness(resps, false);
    NG_RETURN_IF_ERROR(result);
    ResultBuilder builder;
    builder.state(result.value());

    auto& responses = resps.responses();
    VLOG(1) << "Resp size: " << responses.size();
    List list;
    for (auto& resp : responses) {
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

}   // namespace graph
}   // namespace nebula
