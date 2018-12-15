/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/ExecutionPlan.h"

namespace vesoft {
namespace vgraph {

void ExecutionPlan::execute() {
    auto *rctx = ectx()->rctx();
    FLOG_INFO("Parsing query: %s", rctx->query().c_str());

    Status status;
    do {
        auto result = GQLParser().parse(rctx->query());
        if (!result.ok()) {
            status = std::move(result).status();
            break;
        }

        compound_ = std::move(result).value();
        executor_ = std::make_unique<CompoundExecutor>(compound_.get(), ectx());
        status = executor_->prepare();
        if (!status.ok()) {
            break;
        }
    } while (false);

    // Prepare failed
    if (!status.ok()) {
        onError(std::move(status));
        return;
    }

    // Prepared
    auto onFinish = [this] () {
        this->onFinish();
    };
    auto onError = [this] (Status s) {
        this->onError(std::move(s));
    };
    executor_->setOnFinish(std::move(onFinish));
    executor_->setOnError(std::move(onError));

    executor_->execute();
}


void ExecutionPlan::onFinish() {
    executor_->setupResponse(ectx()->rctx()->resp());
    ectx()->rctx()->finish();

    // The `ExecutionPlan' is the root node holding all resources during the execution.
    // When the whole query process is done, it's safe to release this object, as long as
    // no other contexts have chances to access these resources later on,
    // e.g. previously launched uncompleted async sub-tasks, EVEN on failures.
    delete this;
}


void ExecutionPlan::onError(Status status) {
    auto *rctx = ectx()->rctx();
    if (status.isSyntaxError()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_SYNTAX_ERROR);
    } else {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    rctx->resp().set_error_msg(status.toString());
    rctx->finish();
    delete this;
}

}   // namespace vgraph
}   // namespace vesoft
