/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "graph/TruncateSpaceExecutor.h"

namespace nebula {
namespace graph {

TruncateSpaceExecutor::TruncateSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
        : Executor(ectx) {
    sentence_ = static_cast<TruncateSpaceSentence*>(sentence);
}

Status TruncateSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    return Status::OK();
}

void TruncateSpaceExecutor::getSpace() {
    auto future = ectx()->getMetaClient()->getSpace(*spaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(Status::Error("Space not found"));
            return;
        }

        fromSpaceId_ = resp.value().get_space_id();
        auto properties = resp.value().get_properties();
        auto partitionNum = properties.get_partition_num();
        auto replicaFactor = properties.get_replica_factor();
        createSpace(partitionNum, replicaFactor);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Describe space exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void TruncateSpaceExecutor::createSpace(int32_t partsNum, int32_t replicaFactor) {
    tempSpaceName_ = folly::stringPrintf("temp_to_space_%ld",
                                         time::WallClock::fastNowInMicroSec());
    auto future = ectx()->getMetaClient()->createSpace(
            tempSpaceName_, partsNum, replicaFactor, false);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(Status::Error("Create space `%s' failed: %s.",
                                   tempSpaceName_.c_str(), resp.status().toString().c_str()));
            return;
        }
        newSpaceId_ = std::move(resp).value();
        if (newSpaceId_ <= 0) {
            onError_(Status::Error("Create space failed"));
            return;
        }
        copySpace();
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Create space `%s' exception: %s.",
                                        spaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        onError_(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void TruncateSpaceExecutor::copySpace() {
    auto future = ectx()->getMetaClient()->copySchemaFromSpace(
            tempSpaceName_, *spaceName_, true);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(Status::Error("Copy space `%s' failed: %s.",
                                  spaceName_->c_str(), resp.status().toString().c_str()));
            return;
        }
        dropSpace(*spaceName_, false);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Copy space `%s' exception: %s.",
                                       spaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        onError_(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void TruncateSpaceExecutor::renameSpace() {
    auto future = ectx()->getMetaClient()->renameSpace(tempSpaceName_, *spaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(std::move(resp).status());
            return;
        }
        auto  ret = std::move(resp).value();
        if (!ret) {
            LOG(ERROR) << "Rename space " << tempSpaceName_ << " failed";
            auto outFmt = "Rename space `%s' failed. Your need to "
                          "rename the space `%s' to `%s' by yourself when the metad is OK";
            auto error = folly::stringPrintf(outFmt, spaceName_->c_str(),
                    tempSpaceName_.c_str(), spaceName_->c_str());
            onError_(Status::Error(error));
            return;
        }

        // If current space is the truncate space, need to update the spaceId in session
        auto useSpaceId = ectx()->rctx()->session()->space();
        if (useSpaceId > 0 && fromSpaceId_ == useSpaceId) {
            VLOG(1) << "Update current spaceId to " << newSpaceId_;
            ectx()->rctx()->session()->setSpace(*spaceName_, newSpaceId_);
        }

        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Rename space `%s' exception: %s",
                                       tempSpaceName_.c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        onError_(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void TruncateSpaceExecutor::dropSpace(const std::string &spaceName, bool isIfExists) {
    auto future = ectx()->getMetaClient()->dropSpace(spaceName, isIfExists);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [spaceName, isIfExists, this] (auto &&resp) {
        // if drop from_space failed, need to drop the temp_space
        if (!resp.ok()) {
            if (!isIfExists) {
                dropSpace(tempSpaceName_, true);
            }
            onError_(std::move(resp).status());
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            if (!isIfExists) {
                dropSpace(tempSpaceName_, true);
            }
            onError_(Status::Error("Drop space `%s' failed.", spaceName.c_str()));
            return;
        }

        if (!isIfExists) {
            renameSpace();
        } else {
            onError_(Status::Error("Drop space `%s' failed.", spaceName.c_str()));
        }
        return;
    };

    auto error = [spaceName, this] (auto &&e) {
        auto msg = folly::stringPrintf("Drop space `%s' exception: %s",
                                       spaceName.c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        dropSpace(tempSpaceName_, true);
        onError_(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void TruncateSpaceExecutor::execute() {
    // step1: copy schema from space
    // step2: drop from_space
    // step3: rename to_space as from_space
    getSpace();
}

}   // namespace graph
}   // namespace nebula
