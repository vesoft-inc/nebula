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
    auto ovalue = sentence_->vid()->eval();
    auto v = ovalue.value();
    if (!Expression::isInt(v)) {
        return Status::Error("Vertex ID should be of type integer");
    }
    vid_ = Expression::asInt(v);
    return Status::OK();
}

void DeleteVertexExecutor::execute() {
    GraphSpaceID space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->getEdgeKeys(space, vid_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
        }
        auto rpcResp = std::move(resp).value();
        deleteEdges(rpcResp.get_edge_keys());
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
    GraphSpaceID space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->deleteEdges(space, *edges);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
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
    GraphSpaceID space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->deleteVertex(space, vid_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
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

