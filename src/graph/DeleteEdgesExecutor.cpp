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
    : Executor(ectx, "delete_edge") {
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

    status = setupEdgeKeys(edgeType);
    if (!status.ok()) {
        doError(std::move(status));
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

Status DeleteEdgesExecutor::setupEdgeKeys(EdgeType edgeType) {
    auto status = Status::OK();
    auto edgeKeysExpr = sentence_->keys()->keys();
    Getters getters;
    for (auto *keyExpr : edgeKeysExpr) {
        auto *srcExpr = keyExpr->srcid();
        srcExpr->setContext(expCtx_.get());

        auto *dstExpr = keyExpr->dstid();
        dstExpr->setContext(expCtx_.get());

        auto rank = keyExpr->rank();
        status = srcExpr->prepare();
        if (!status.ok()) {
            break;
        }
        status = dstExpr->prepare();
        if (!status.ok()) {
            break;
        }
        auto value = srcExpr->eval(getters);
        if (!value.ok()) {
            return value.status();
        }
        auto srcid = value.value();
        value = dstExpr->eval(getters);
        if (!value.ok()) {
            return value.status();
        }
        auto dstid = value.value();
        if (!Expression::isInt(srcid) || !Expression::isInt(dstid)) {
            status = Status::Error("ID should be of type integer.");
            break;
        }

        storage::cpp2::EdgeKey outkey;
        outkey.set_src(Expression::asInt(srcid));
        outkey.set_edge_type(edgeType);
        outkey.set_dst(Expression::asInt(dstid));
        outkey.set_ranking(rank);

        storage::cpp2::EdgeKey inkey;
        inkey.set_src(Expression::asInt(dstid));
        inkey.set_edge_type(-edgeType);
        inkey.set_dst(Expression::asInt(srcid));
        inkey.set_ranking(rank);

        edgeKeys_.emplace_back(std::move(outkey));
        edgeKeys_.emplace_back(std::move(inkey));
    }
    return status;
}

}   // namespace graph
}   // namespace nebula

