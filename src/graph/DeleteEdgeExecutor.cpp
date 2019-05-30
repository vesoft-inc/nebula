/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/DeleteEdgeExecutor.h"
#include "meta/SchemaManager.h"
#include "filter/Expressions.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

DeleteEdgeExecutor::DeleteEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DeleteEdgeSentence*>(sentence);
}

Status DeleteEdgeExecutor::prepare() {
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        auto spaceId = ectx()->rctx()->session()->space();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *sentence_->edge());
        if (!edgeStatus.ok()) {
            status = edgeStatus.status();
            break;
        }
        edgeType_ = edgeStatus.value();
        auto schema = ectx()->schemaManager()->getEdgeSchema(spaceId, edgeType_);
        if (schema == nullptr) {
            status = Status::Error("No schema found for '%s'", sentence_->edge()->c_str());
            break;
        }

        edgeKeys_ = sentence_->keys();
        auto *clause = sentence_->whereClause();
        if (clause != nullptr) {
            filter_ = clause->filter();
        }
    } while (false);

    return status;
}

void DeleteEdgeExecutor::execute() {
    std::vector<storage::cpp2::EdgeKey> edges(edgeKeys_.size() * 2);  // inbound and outbound
    auto index = 0;
    for (auto &item : edgeKeys_) {
        auto src = item->srcid();
        auto dst = item->dstid();
        auto rank = item->rank();
        {
            auto &out = edges[index++];
            out.set_src(src);
            out.set_dst(dst);
            out.set_ranking(rank);
            out.set_edge_type(edgeType_);
        }
        {
            auto &in = edges[index++];
            in.set_src(dst);
            in.set_dst(src);
            in.set_ranking(rank);
            in.set_edge_type(-edgeType_);
        }
    }

    auto space = ectx()->rctx()->session()->space();
    std::string filterStr = filter_ != nullptr ? Expression::encode(filter_) : "";
    // TODO(zlcook) Need to consider distributed transaction because in-edges/out-edges
    // may be in different partitions
    auto future = ectx()->storage()->deleteEdges(space, std::move(edges), filterStr);

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            // TODO(zlcook) Need to consider atomic issues
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
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
