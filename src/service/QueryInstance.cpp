/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/QueryInstance.h"

#include "common/base/Base.h"

#include "exec/ExecutionError.h"
#include "exec/Executor.h"
#include "planner/ExecutionPlan.h"
#include "planner/PlanNode.h"
#include "scheduler/Scheduler.h"

namespace nebula {
namespace graph {

void QueryInstance::execute() {
    auto *rctx = qctx()->rctx();
    VLOG(1) << "Parsing query: " << rctx->query();

    Status status;
    do {
        auto result = GQLParser().parse(rctx->query());
        if (!result.ok()) {
            status = std::move(result).status();
            LOG(ERROR) << "Parser: `" << rctx->query() << "' error: " << status;
            break;
        }

        sentences_ = std::move(result).value();
        validator_ = std::make_unique<ASTValidator>(sentences_.get(), qctx());
        status = validator_->validate();
        if (!status.ok()) {
            break;
        }

        // TODO: optional optimize for plan.
    } while (false);

    if (!status.ok()) {
        LOG(ERROR) << status;
        onError(std::move(status));
        return;
    }

    scheduler_->schedule()
        .then([this](Status s) {
            if (s.ok()) {
                this->onFinish();
            } else {
                this->onError(std::move(s));
            }
        })
        .onError([this](const ExecutionError &e) { onError(e.status()); })
        .onError([this](const std::exception &e) { onError(Status::Error("%s", e.what())); });
}

void QueryInstance::onFinish() {
    auto *rctx = qctx()->rctx();
    // executor_->setupResponse(rctx->resp());
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().set_latency_in_us(latency);
    auto &spaceName = rctx->session()->spaceName();
    rctx->resp().set_space_name(spaceName);
    auto&& value = qctx()->ectx()->moveValue(qctx()->plan()->root()->varName());
    if (value.type() == Value::Type::DATASET) {
        auto result = value.moveDataSet();
        if (!result.colNames.empty()) {
            rctx->resp().set_data(std::move(result));
        } else {
            LOG(ERROR) << "Empty column name list";
            rctx->resp().set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
            rctx->resp().set_error_msg("Internal error: empty column name list");
        }
    }
    rctx->finish();

    // The `QueryInstance' is the root node holding all resources during the execution.
    // When the whole query process is done, it's safe to release this object, as long as
    // no other contexts have chances to access these resources later on,
    // e.g. previously launched uncompleted async sub-tasks, EVEN on failures.
    delete this;
}

void QueryInstance::onError(Status status) {
    auto *rctx = qctx()->rctx();
    if (status.isSyntaxError()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_SYNTAX_ERROR);
    } else if (status.isStatementEmpty()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_STATEMENT_EMTPY);
    } else {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    auto &spaceName = rctx->session()->spaceName();
    rctx->resp().set_space_name(spaceName);
    rctx->resp().set_error_msg(status.toString());
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().set_latency_in_us(latency);
    rctx->finish();
    delete this;
}

}   // namespace graph
}   // namespace nebula
