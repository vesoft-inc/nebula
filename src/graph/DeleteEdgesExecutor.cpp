/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DeleteEdgesExecutor.h"
#include "meta/SchemaManager.h"
#include "filter/Expressions.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

DeleteEdgesExecutor::DeleteEdgesExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
    : TraverseExecutor(ectx, "delete_edge") {
    sentence_ = static_cast<DeleteEdgesSentence*>(sentence);
}

Status DeleteEdgesExecutor::prepare() {
    return Status::OK();
}

void DeleteEdgesExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    auto space = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setSpace(space);
    expCtx_->setStorageClient(ectx()->getStorageClient());

    auto edgeStatus = ectx()->schemaManager()->toEdgeType(space, *sentence_->edge());
    if (!edgeStatus.ok()) {
        doError(edgeStatus.status());
        return;
    }

    auto edgeType = edgeStatus.value();
    auto schema = ectx()->schemaManager()->getEdgeSchema(space, edgeType);
    if (schema == nullptr) {
        status = Status::Error("No schema found for '%s'", sentence_->edge()->c_str());
        doError(std::move(status));
        return;
    }

    auto ret = getEdgeKeys(expCtx_.get(), sentence_, edgeType, false);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        doError(std::move(ret).status());
        return;
    }

    edgeKeys_ = std::move(ret).value();
    if (edgeKeys_.empty()) {
        onEmptyInputs();
        return;
    }
    // TODO Need to consider distributed transaction because in-edges/out-edges
    // may be in different partitions
    auto future = ectx()->getStorageClient()->deleteEdges(space, std::move(edgeKeys_));

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            // TODO Need to consider atomic issues
            doError(Status::Error("Delete edge not complete, completeness: %d", completeness));
            return;
        }
        doFinish(Executor::ProcessControl::kNext, edgeKeys_.size());
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Delete edge exception: " << e.what();
        doError(Status::Error("Delete edge exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteEdgesExecutor::onEmptyInputs() {
    auto outputs = std::make_unique<InterimResult>();
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext);
}
}   // namespace graph
}   // namespace nebula

