/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateSpaceExecutor.h"

namespace nebula {
namespace graph {

CreateSpaceExecutor::CreateSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
    : Executor(ectx, "create_space") {
    sentence_ = static_cast<CreateSpaceSentence*>(sentence);
}


Status CreateSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    for (auto &item : sentence_->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM:
                partNum_ = item->get_partition_num();
                if (partNum_ <= 0) {
                    return Status::Error("Partition_num value should be greater than zero");
                }
                break;
            case SpaceOptItem::REPLICA_FACTOR:
                replicaFactor_ = item->get_replica_factor();
                if (replicaFactor_ <= 0) {
                    return Status::Error("Replica_factor value should be greater than zero");
                }
                break;
        }
    }
    return Status::OK();
}


void CreateSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->createSpace(
        *spaceName_, partNum_, replicaFactor_, sentence_->isIfNotExist());
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Create space `%s' failed: %s.",
                                    spaceName_->c_str(), resp.status().toString().c_str()));
            return;
        }
        auto spaceId = std::move(resp).value();
        if (spaceId <= 0) {
            doError(Status::Error("Create space failed"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Create space `%s' exception: %s.",
                                        spaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(msg));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
