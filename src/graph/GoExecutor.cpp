/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/GoExecutor.h"


namespace nebula {
namespace graph {

GoExecutor::GoExecutor(Sentence *sentence, ExecutionContext *ectx) : TraverseExecutor(ectx) {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<GoSentence*>(sentence);
}


Status GoExecutor::prepare() {
    DCHECK(sentence_ != nullptr);
    Status status;
    expctx_ = std::make_unique<ExpressionContext>();
    do {
        status = prepareStep();
        if (!status.ok()) {
            break;
        }
        status = prepareFrom();
        if (!status.ok()) {
            break;
        }
        status = prepareOver();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
        status = prepareYield();
        if (!status.ok()) {
            break;
        }
        status = prepareResultSchema();
        if (!status.ok()) {
            break;
        }
        status = prepareNeededProps();
        if (!status.ok()) {
            break;
        }
    } while (false);

    if (!status.ok()) {
        return status;
    }

    expctx_->print();

    if (!onResult_) {
        onResult_ = [this] (TraverseRecords records) {
            this->cacheResult(std::move(records));
        };
    }

    return status;
}


void GoExecutor::execute() {
    FLOG_INFO("Executing Go: %s", sentence_->toString().c_str());
    using Result = StatusOr<storage::cpp2::QueryResponse>;
    auto eprops = expctx_->edgePropNames();
    auto vprops = expctx_->srcNodePropNames();
    auto future = ectx()->storage()->getOutBound(starts_, edge_, eprops, vprops);
    auto *runner = ectx()->rctx()->runner();
    std::move(future).via(runner).then([this] (Result result) {
        if (!result.ok()) {
            auto &resp = ectx()->rctx()->resp();
            auto status = result.status();
            resp.set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
            resp.set_error_msg(status.toString());
        }
        if (onFinish_) {
            onFinish_();
        }
    });
}


void GoExecutor::feedResult(TraverseRecords records) {
    inputs_.reserve(records.size());
    for (auto &record : records) {
        inputs_.push_back(std::move(record));
    }
}


Status GoExecutor::prepareResultSchema() {
    resultSchema_ = std::make_unique<ResultSchema>();
    for (auto *column : yields_) {
        resultSchema_->addColumn(*column->alias());
    }
    return Status::OK();
}


Status GoExecutor::prepareStep() {
    auto *clause = sentence_->stepClause();
    if (clause != nullptr) {
        steps_ = clause->steps();
        upto_ = clause->isUpto();
    }
    return Status::OK();
}


Status GoExecutor::prepareFrom() {
    Status status = Status::OK();
    auto *clause = sentence_->fromClause();
    do {
        if (clause == nullptr) {
            break;
        }
        auto *alias = clause->alias();
        if (alias == nullptr) {
            break;
        }
        expctx_->addAlias(*alias, AliasKind::SourceNode);
        starts_ = clause->srcNodeList()->nodeIds();
    } while (false);
    return status;
}


Status GoExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    do {
        if (clause == nullptr) {
            break;
        }
        edge_ = clause->edge();
        reversely_ = clause->isReversely();
        if (clause->alias() != nullptr) {
            expctx_->addAlias(*clause->alias(), AliasKind::Edge, *clause->edge());
        }
    } while (false);
    return status;
}


Status GoExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    if (clause != nullptr) {
        filter_ = clause->filter();
    }
    return Status::OK();
}


Status GoExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        yields_ = clause->columns();
    }
    return Status::OK();
}


Status GoExecutor::prepareNeededProps() {
    auto status = Status::OK();
    do {
        if (filter_ != nullptr) {
            filter_->setContext(expctx_.get());
            status = filter_->prepare();
            if (!status.ok()) {
                break;
            }
        }
        if (yields_.empty()) {
            break;
        }
        for (auto *col : yields_) {
            col->expr()->setContext(expctx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
        }
    } while (false);

    return status;
}


void GoExecutor::cacheResult(TraverseRecords records) {
    UNUSED(records);
}


void GoExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    UNUSED(resp);
}

}   // namespace graph
}   // namespace nebula
