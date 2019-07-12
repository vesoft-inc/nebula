/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/FetchVerticesExecutor.h"

namespace nebula {
namespace graph {
FetchVerticesExecutor::FetchVerticesExecutor(Sentence *sentence, ExecutionContext *ectx)
        :TraverseExecutor(ectx) {
    sentence_ = static_cast<FetchVerticesSentence*>(sentence);
}

Status FetchVerticesExecutor::prepare() {
    DCHECK_NOTNULL(sentence_);
    Status status = Status::OK();
    do {
        if (sentence_->isRef()) {
            auto *expr = sentence_->ref();
            if (expr->isInputExpression()) {
                auto *iexpr = static_cast<InputPropertyExpression*>(expr);
                colname_ = iexpr->prop();
            } else if (expr->isVariableExpression()) {
                auto *vexpr = static_cast<VariablePropertyExpression*>(expr);
                varname_ = vexpr->var();
                colname_ = vexpr->prop();
            } else {
                LOG(FATAL) << "Unknown kind of expression.";
            }
        }

        auto vidList = sentence_->vidList();
        for (auto *expr : vidList) {
            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval();
            if (!Expression::isInt(value)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            vids_.push_back(Expression::asInt(value));
        }
    } while (false);

    return status;
}

Status FetchVerticesExecutor::prepareYield() {
    Status status = Status::OK();
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        auto yields = clause->columns();
        for (auto *col : yields) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
        }
    } else {
        // TODO
    }

    return Status::OK();
}

void FetchVerticesExecutor::execute() {
    FLOG_INFO("Executing FetchVertices: %s", sentence_->toString().c_str());
    auto status = setupVids();
    if (!status.ok()) {
        onError_(std::move(status));
        return;
    }
    if (vids_.empty()) {
        onEmptyInputs();
    }

    fetchVertices();
}

void FetchVerticesExecutor::fetchVertices() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getPropNames();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(Status::Error("Get prop names error."));
    }

    auto props = status.value();
    auto future = ectx()->storage()->getVertexProps(spaceId, vids_, std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            DCHECK(onError_);
            onError_(Status::Error("Get props failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get vertices partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        processResult(std::move(result));
        return;
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

StatusOr<std::vector<storage::cpp2::PropDef>> FetchVerticesExecutor::getPropNames() {
    std::vector<storage::cpp2::PropDef> props;
    // TODO
    return props;
}

void FetchVerticesExecutor::processResult(RpcResponse &&result) {
    UNUSED(result);
}

void FetchVerticesExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}

Status FetchVerticesExecutor::setupVids() {
    if (!vids_.empty()) {
        return Status::OK();
    }

    const auto *inputs = inputs_.get();
    // Take one column from a variable
    if (varname_ != nullptr) {
        auto *varInputs = ectx()->variableHolder()->get(*varname_);
        if (varInputs == nullptr) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
        DCHECK(inputs == nullptr);
        inputs = varInputs;
    }
    // No error happened, but we are having empty inputs
    if (inputs == nullptr) {
        return Status::OK();
    }

    auto result = inputs->getVIDs(*colname_);
    if (!result.ok()) {
        return std::move(result).status();
    }
    vids_ = std::move(result).value();
    return Status::OK();
}

void FetchVerticesExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    inputs_ = std::move(result);
}

void FetchVerticesExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}
}  // namespace graph
}  // namespace nebula
