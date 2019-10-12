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
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DeleteVertexSentence*>(sentence);
}

Status DeleteVertexExecutor::prepare() {
    spaceId_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setSpace(spaceId_);
    expCtx_->setStorageClient(ectx()->getStorageClient());

    auto vid = sentence_->vid();
    vid->setContext(expCtx_.get());
    auto ovalue = vid->eval();
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
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
        }
        auto rpcResp = std::move(resp).value();
        std::vector<storage::cpp2::EdgeKey> allEdges;
        for (auto& edge : *rpcResp.get_edge_keys()) {
            auto reverseEdge = storage::cpp2::EdgeKey(apache::thrift::FragileConstructor::FRAGILE,
                                                      edge.get_dst(),
                                                      -(edge.get_edge_type()),
                                                      edge.get_ranking(),
                                                      edge.get_src());
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
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
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
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
        }
        deleteVertex();
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVertexExecutor::deleteVertex() {
    auto future = ectx()->getStorageClient()->deleteVertex(spaceId_, vid_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

