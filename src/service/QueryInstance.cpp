/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

#include "service/QueryInstance.h"

#include "exec/ExecutionError.h"
#include "exec/Executor.h"

namespace nebula {
namespace graph {

void QueryInstance::execute() {
    auto *rctx = ectx()->rctx();
    FLOG_INFO("Parsing query: %s", rctx->query().c_str());

    Status status;
    auto plan = std::make_unique<ExecutionPlan>(ectx());
    do {
        auto result = GQLParser().parse(rctx->query());
        if (!result.ok()) {
            status = std::move(result).status();
            LOG(ERROR) << status;
            break;
        }

        sentences_ = std::move(result).value();
        validator_ = std::make_unique<ASTValidator>(
            sentences_.get(), rctx->session(), ectx()->schemaManager());
        status = validator_->validate(plan.get());
        if (!status.ok()) {
            LOG(ERROR) << status;
            break;
        }

        // TODO: optional optimize for plan.
    } while (false);

    if (!status.ok()) {
        onError(std::move(status));
        return;
    }

    auto executor = plan->createExecutor();
    status = executor->prepare();
    if (!status.ok()) {
        onError(std::move(status));
        return;
    }
    executor->execute()
        .then([this](Status s) {
            if (s.ok()) {
                this->onFinish();
            } else {
                this->onError(std::move(s));
            }
        })
        .onError([this](const ExecutionError &e) { onError(e.status()); })
        .onError([this](const std::exception &e) { onError(Status::Error("%s", e.what())); })
        .ensure([p = std::move(plan)]() mutable { p.reset(); });
}

void QueryInstance::onFinish() {
    auto *rctx = ectx()->rctx();
    // executor_->setupResponse(rctx->resp());
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().set_latency_in_us(latency);
    auto &spaceName = rctx->session()->spaceName();
    rctx->resp().set_space_name(spaceName);
    rctx->finish();

    // The `QueryInstance' is the root node holding all resources during the execution.
    // When the whole query process is done, it's safe to release this object, as long as
    // no other contexts have chances to access these resources later on,
    // e.g. previously launched uncompleted async sub-tasks, EVEN on failures.
    delete this;
}

void QueryInstance::onError(Status status) {
    auto *rctx = ectx()->rctx();
    if (status.isSyntaxError()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_SYNTAX_ERROR);
    } else if (status.isStatementEmpty()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_STATEMENT_EMTPY);
    } else {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    // rctx->resp().set_error_msg(status.toString());
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().set_latency_in_us(latency);
    rctx->finish();
    delete this;
}

}   // namespace graph
}   // namespace nebula
