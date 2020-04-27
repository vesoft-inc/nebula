/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/MultiOutputsExecutor.h"

#include "planner/Query.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

Status MultiOutputsExecutor::prepare() {
    outCount_++;
    if (!prepared_) {
        prepared_ = true;
        return input_->prepare();
    }

    return Status::OK();
}

folly::Future<Status> MultiOutputsExecutor::execute() {
    folly::SpinLockGuard g(lock_);

    if (currentOut_ == 0) {
        DCHECK_GT(outCount_, 0);
        currentOut_ = outCount_;
        sharedPromise_ = std::make_shared<folly::SharedPromise<Status>>();
    }

    if (--currentOut_ > 0) {
        return sharedPromise_->getFuture();
    }

    return input_->execute().then(cb([this](Status s) {
        dumpLog();

        sharedPromise_->setValue(s);
        return s;
    }));
}

}   // namespace graph
}   // namespace nebula
