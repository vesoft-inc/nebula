/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DeleteVerticesExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

DeleteVerticesExecutor::DeleteVerticesExecutor(Sentence *sentence,
                                               ExecutionContext *ectx)
    : Executor(ectx, "delete_vertices") {
    sentence_ = static_cast<DeleteVerticesSentence*>(sentence);
}

Status DeleteVerticesExecutor::prepare() {
    return Status::OK();
}

void DeleteVerticesExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    space_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setSpace(space_);
    expCtx_->setStorageClient(ectx()->getStorageClient());

    auto vidList = sentence_->vidList();
    vidList->setContext(expCtx_.get());
    status = vidList->prepare();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    auto values = vidList->eval();
    for (auto value : values) {
        auto v = value.value();
        if (!Expression::isInt(v)) {
            status = Status::Error("Vertex ID should be of type integer");
            doError(std::move(status));
            return;
        }
        auto vid = Expression::asInt(v);
        vids_.emplace_back(vid);
    }

    // TODO(zlcook) Get edgeKeys of a vertex by Go
    auto future = ectx()->getStorageClient()->getEdgeKeys(space_, vids_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness != 100) {
            LOG(ERROR) << "Get vertices partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << " error code: " << static_cast<int>(error.second);
                doError(Status::Error("Delete edge not complete, completeness: %d",
                                      completeness));
                return;
            }
        }

        auto rpcResp = std::move(result).responses();
        std::vector<storage::cpp2::EdgeKey> allEdges;
        for (auto& response : rpcResp) {
            auto keys = response.get_edge_keys();
            for (auto iter = keys->begin(); iter != keys->end(); iter++) {
                for (auto& edge : iter->second) {
                    storage::cpp2::EdgeKey reverseEdge;
                    reverseEdge.set_src(edge.get_dst());
                    reverseEdge.set_edge_type(-(edge.get_edge_type()));
                    reverseEdge.set_ranking(edge.get_ranking());
                    reverseEdge.set_dst(edge.get_src());

                    allEdges.emplace_back(std::move(edge));
                    allEdges.emplace_back(std::move(reverseEdge));
                }
            }
        }

        if (allEdges.size() > 0) {
            deleteEdges(allEdges);
        } else {
            deleteVertices();
        }
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVerticesExecutor::deleteEdges(std::vector<storage::cpp2::EdgeKey>& edges) {
    auto future = ectx()->getStorageClient()->deleteEdges(space_, edges);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            LOG(ERROR) << "Delete edges partially failed: "  << completeness << "%";
            doError(Status::Error("Internal error"));
            return;
        }
        deleteVertices();
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVerticesExecutor::deleteVertices() {
    auto future = ectx()->getStorageClient()->deleteVertices(space_, vids_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            LOG(ERROR) << "Delete vertices partially failed: "  << completeness << "%";
            doError(Status::Error("Internal error"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext, vids_.size());
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

