/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/QueryInstance.h"

#include "common/base/Base.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "optimizer/OptRule.h"
#include "parser/ExplainSentence.h"
#include "planner/ExecutionPlan.h"
#include "planner/PlanNode.h"
#include "scheduler/Scheduler.h"
#include "util/ScopedTimer.h"
#include "validator/Validator.h"
#include "util/AstUtils.h"
#include "stats/StatsDef.h"

using nebula::opt::Optimizer;
using nebula::opt::OptRule;
using nebula::opt::RuleSet;


namespace nebula {
namespace graph {

QueryInstance::QueryInstance(std::unique_ptr<QueryContext> qctx, Optimizer *optimizer) {
    qctx_ = std::move(qctx);
    optimizer_ = DCHECK_NOTNULL(optimizer);
    scheduler_ = std::make_unique<Scheduler>(qctx_.get());
}


void QueryInstance::execute() {
    Status status = validateAndOptimize();
    if (!status.ok()) {
        onError(std::move(status));
        return;
    }

    if (!explainOrContinue()) {
        onFinish();
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


Status QueryInstance::validateAndOptimize() {
    auto *rctx = qctx()->rctx();
    VLOG(1) << "Parsing query: " << rctx->query();
    auto result = GQLParser(qctx()).parse(rctx->query());
    NG_RETURN_IF_ERROR(result);
    sentence_ = std::move(result).value();

    NG_RETURN_IF_ERROR(Validator::validate(sentence_.get(), qctx()));

    auto plan = std::make_unique<ExecutionPlan>();
    {
        SCOPED_TIMER(plan->optimizeTimeInUs());
        auto rootStatus = optimizer_->findBestPlan(qctx_.get());
        NG_RETURN_IF_ERROR(rootStatus);
        plan->setRoot(const_cast<PlanNode *>(std::move(rootStatus).value()));
    }
    qctx_->setPlan(std::move(plan));

    return Status::OK();
}


bool QueryInstance::explainOrContinue() {
    if (sentence_->kind() != Sentence::Kind::kExplain) {
        return true;
    }
    qctx_->fillPlanDescription();
    return static_cast<const ExplainSentence *>(sentence_.get())->isProfile();
}


void QueryInstance::onFinish() {
    auto rctx = qctx()->rctx();
    VLOG(1) << "Finish query: " << rctx->query();
    auto ectx = qctx()->ectx();
    auto &spaceName = rctx->session()->space().name;
    rctx->resp().spaceName = std::make_unique<std::string>(spaceName);
    auto name = qctx()->plan()->root()->outputVar();
    if (ectx->exist(name)) {
        auto &&value = ectx->moveValue(name);
        if (value.type() == Value::Type::DATASET) {
            auto result = value.moveDataSet();
            if (!result.colNames.empty()) {
                rctx->resp().data = std::make_unique<DataSet>(std::move(result));
            } else {
                LOG(ERROR) << "Empty column name list";
                rctx->resp().errorCode = ErrorCode::E_EXECUTION_ERROR;
                rctx->resp().errorMsg = std::make_unique<std::string>(
                    "Internal error: empty column name list");
            }
        }
    }

    if (qctx()->planDescription() != nullptr) {
        rctx->resp().planDesc = std::make_unique<PlanDescription>(
            std::move(*qctx()->planDescription()));
    }

    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().latencyInUs = latency;
    stats::StatsManager::addValue(kQueryLatencyUs, latency);
    if (latency > static_cast<uint64_t>(FLAGS_slow_query_threshold_us)) {
        stats::StatsManager::addValue(kNumSlowQueries);
        stats::StatsManager::addValue(kSlowQueryLatencyUs, latency);
    }
    rctx->finish();

    // The `QueryInstance' is the root node holding all resources during the execution.
    // When the whole query process is done, it's safe to release this object, as long as
    // no other contexts have chances to access these resources later on,
    // e.g. previously launched uncompleted async sub-tasks, EVEN on failures.
    delete this;
}


void QueryInstance::onError(Status status) {
    LOG(ERROR) << status;
    auto *rctx = qctx()->rctx();
    switch (status.code()) {
        case Status::Code::kOk:
            rctx->resp().errorCode = ErrorCode::SUCCEEDED;
            break;
        case Status::Code::kSyntaxError:
            rctx->resp().errorCode = ErrorCode::E_SYNTAX_ERROR;
            break;
        case Status::Code::kStatementEmpty:
            rctx->resp().errorCode = ErrorCode::E_STATEMENT_EMPTY;
            break;
        case Status::Code::kSemanticError:
            rctx->resp().errorCode = ErrorCode::E_SEMANTIC_ERROR;
            break;
        case Status::Code::kPermissionError:
            rctx->resp().errorCode = ErrorCode::E_BAD_PERMISSION;
            break;
        case Status::Code::kBalanced:
        case Status::Code::kEdgeNotFound:
        case Status::Code::kError:
        case Status::Code::kHostNotFound:
        case Status::Code::kIndexNotFound:
        case Status::Code::kInserted:
        case Status::Code::kKeyNotFound:
        case Status::Code::kPartialSuccess:
        case Status::Code::kLeaderChanged:
        case Status::Code::kNoSuchFile:
        case Status::Code::kNotSupported:
        case Status::Code::kPartNotFound:
        case Status::Code::kSpaceNotFound:
        case Status::Code::kGroupNotFound:
        case Status::Code::kZoneNotFound:
        case Status::Code::kTagNotFound:
        case Status::Code::kUserNotFound:
        case Status::Code::kListenerNotFound:
            rctx->resp().errorCode = ErrorCode::E_EXECUTION_ERROR;
            break;
    }
    auto &spaceName = rctx->session()->space().name;
    rctx->resp().spaceName = std::make_unique<std::string>(spaceName);
    rctx->resp().errorMsg = std::make_unique<std::string>(status.toString());
    auto latency = rctx->duration().elapsedInUSec();
    rctx->resp().latencyInUs = latency;
    stats::StatsManager::addValue(kQueryLatencyUs, latency);
    stats::StatsManager::addValue(kNumQueryErrors);
    if (latency > static_cast<uint64_t>(FLAGS_slow_query_threshold_us)) {
        stats::StatsManager::addValue(kNumSlowQueries);
        stats::StatsManager::addValue(kSlowQueryLatencyUs, latency);
    }
    rctx->finish();
    delete this;
}

}   // namespace graph
}   // namespace nebula
