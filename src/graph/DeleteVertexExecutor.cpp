/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DeleteVertexExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

DeleteVertexExecutor::DeleteVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx)
    : Executor(ectx, "delete_vertex") {
    sentence_ = static_cast<DeleteVertexSentence*>(sentence);
}

Status DeleteVertexExecutor::prepare() {
    spaceId_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setSpace(spaceId_);
    expCtx_->setStorageClient(ectx()->getStorageClient());

    auto vid = sentence_->vid();
    vid->setContext(expCtx_.get());
    Getters getters;
    auto ovalue = vid->eval(getters);
    auto v = ovalue.value();
    if (!Expression::isInt(v)) {
        return Status::Error("Vertex ID should be of type integer");
    }
    vid_ = Expression::asInt(v);
    return Status::OK();
}

void DeleteVertexExecutor::execute() {
    // TODO(zlcook) Get edgeKeys of a vertex by Go
    auto future = ectx()->getStorageClient()->getEdgeKeys(spaceId_, vid_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Get edge key failed when delete vertex `%ld'.", vid_));
            return;
        }
        auto rpcResp = std::move(resp).value();
        std::vector<storage::cpp2::EdgeKey> allEdges;
        for (auto& edge : *rpcResp.get_edge_keys()) {
            storage::cpp2::EdgeKey reverseEdge;
            reverseEdge.set_src(edge.get_dst());
            reverseEdge.set_edge_type(-(edge.get_edge_type()));
            reverseEdge.set_ranking(edge.get_ranking());
            reverseEdge.set_dst(edge.get_src());
            allEdges.emplace_back(std::move(edge));
            allEdges.emplace_back(std::move(reverseEdge));
        }
        if (allEdges.size() > 0) {
            deleteEdges(&allEdges);
        } else {
            deleteVertex();
        }
        return;
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Get edge key exception when delete vertex `%ld': %s.",
                vid_, e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVertexExecutor::deleteEdges(std::vector<storage::cpp2::EdgeKey>* edges) {
    auto future = ectx()->getStorageClient()->deleteEdges(spaceId_, *edges);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            doError(Status::Error(
                        "Delete edge not complete when delete vertex `%ld', completeness: %d",
                        vid_, completeness));
            return;
        }
        deleteVertex();
        return;
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Delete edge exception when delete vertex `%ld': %s",
                vid_, e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(msg));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVertexExecutor::deleteVertex() {
    auto future = ectx()->getStorageClient()->deleteVertex(spaceId_, vid_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Delete vertex `%ld' failed.", vid_));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
        return;
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Delete vertex `%ld' exception: %s",
                vid_, e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(msg));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

