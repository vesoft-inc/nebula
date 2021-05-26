/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/ExecutionPlan.h"
#include "stats/StatsManager.h"

DEFINE_uint32(slow_query_threshold_us, 1000*1000,
        "Slow query's threshold in us, default: 1000*1000us(1s)");

namespace nebula {
namespace graph {

void ExecutionPlan::execute() {
    auto *rctx = ectx()->rctx();
    VLOG(1) << "Parsing query: " << rctx->query().c_str();

    Status status;
    do {
        auto result = GQLParser().parse(rctx->query());
        if (!result.ok()) {
            status = std::move(result).status();
            LOG(ERROR) << "Do cmd `" << rctx->query() << "' failed: " << status;
            stats::Stats::addStatsValue(parseStats_.get(), false);
            break;
        }

        sentences_ = std::move(result).value();
        executor_ = std::make_unique<SequentialExecutor>(sentences_.get(), ectx());
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
    auto onFinish = [this] (Executor::ProcessControl ctr) {
        UNUSED(ctr);
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
    auto *rctx = ectx()->rctx();
    executor_->setupResponse(rctx->resp());
    auto latency = rctx->duration().elapsedInUSec();
    stats::Stats::addStatsValue(allStats_.get(), true, latency);
    rctx->resp().set_latency_in_us(latency);
    auto &spaceName = rctx->session()->spaceName();
    rctx->resp().set_space_name(spaceName);
    if (!ectx()->getWarningMsg().empty()) {
        rctx->resp().set_warning_msg(
                folly::stringPrintf("%s.", folly::join(", ", ectx()->getWarningMsg()).c_str()));
    }

    // slow query
    if (latency >= static_cast<uint32_t>(FLAGS_slow_query_threshold_us)) {
        stats::Stats::addStatsValue(slowQueryStats_.get(), true, latency);
        LOG(WARNING) << "Slow query: exec succ, cost: " << latency
            << "us, space: " << spaceName
            << ", query: " << rctx->query();
    }
    rctx->finish();

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
    } else if (status.isStatementEmpty()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_STATEMENT_EMTPY);
    } else if (status.isPermissionError()) {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_BAD_PERMISSION);
    } else {
        rctx->resp().set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    rctx->resp().set_error_msg(status.toString());
    auto latency = rctx->duration().elapsedInUSec();
    stats::Stats::addStatsValue(allStats_.get(), false, latency);
    rctx->resp().set_latency_in_us(latency);

    auto errorCodeIt = cpp2::_ErrorCode_VALUES_TO_NAMES.find(rctx->resp().get_error_code());
    LOG(ERROR) << "Execute failed! errcode: "
        << (errorCodeIt != cpp2::_ErrorCode_VALUES_TO_NAMES.end()
                ? errorCodeIt->second
                : std::to_string(int32_t(rctx->resp().get_error_code())))
        << ", cost: " << latency
        << "us, space: " << (rctx->session()->spaceName())
        << ", query: " << (rctx->query())
        << ", errmsg: " << status.toString();

    // slow query
    if (latency >= static_cast<uint32_t>(FLAGS_slow_query_threshold_us)) {
        stats::Stats::addStatsValue(slowQueryStats_.get(), false, latency);
        LOG(WARNING) << "Slow query: exec failed! cost: " << latency
            << "us, errcode: "
            << (errorCodeIt != cpp2::_ErrorCode_VALUES_TO_NAMES.end()
                    ? errorCodeIt->second
                    : std::to_string(int32_t(rctx->resp().get_error_code())))
            << ", space: " << (rctx->session()->spaceName())
            << ", query: " << (rctx->query());
    }

    rctx->finish();
    delete this;
}

}   // namespace graph
}   // namespace nebula
