/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/SetExecutor.h"

namespace nebula {
namespace graph {

SetExecutor::SetExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<SetSentence*>(sentence);
}

Status SetExecutor::prepare() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        return status;
    }

    left_ = makeTraverseExecutor(sentence_->left());
    right_ = makeTraverseExecutor(sentence_->right());
    DCHECK(left_ != nullptr);
    DCHECK(right_ != nullptr);


    auto onError = [this] (Status s) {
        this->onError_(std::move(s));
    };

    auto onFinish = [] () {
        return;
    };

    {
        futures_.emplace_back(leftP_.getFuture());
        auto onResult = [this] (std::unique_ptr<InterimResult> result) {
            this->leftResult_ = std::move(result);
            leftP_.setValue();
            VLOG(3) << "Left result set.";
        };
        left_->setOnResult(onResult);
        left_->setOnFinish(onFinish);
        left_->setOnError(onError);
    }
    {
        futures_.emplace_back(rightP_.getFuture());
        auto onResult = [this] (std::unique_ptr<InterimResult> result) {
            this->rightResult_ = std::move(result);
            rightP_.setValue();
            VLOG(3) << "Right result set.";
        };
        right_->setOnResult(onResult);
        right_->setOnFinish(onFinish);
        right_->setOnError(onError);
    }

    status = left_->prepare();
    if (!status.ok()) {
        FLOG_ERROR("Prepare executor `%s' failed: %s",
                    left_->name(), status.toString().c_str());
        return status;
    }

    status = right_->prepare();
    if (!status.ok()) {
        FLOG_ERROR("Prepare executor `%s' failed: %s",
                    right_->name(), status.toString().c_str());
        return status;
    }

    return Status::OK();
}

void SetExecutor::execute() {
    auto *runner = ectx()->rctx()->runner();
    runner->add([this] () mutable { left_->execute(); });
    runner->add([this] () mutable { right_->execute(); });

    auto cb = [this] (auto &&result) {
        UNUSED(result);
        switch (sentence_->op()) {
            case SetSentence::Operator::UNION:
                doUnion();
                break;
            case SetSentence::Operator::INTERSECT:
                doIntersect();
                break;
            case SetSentence::Operator::MINUS:
                doMinus();
                break;
            default:
                LOG(ERROR) << "Unknown operator: " << sentence_->op();
        }
    };
    folly::collectAll(futures_).via(runner).thenValue(cb);
}

void SetExecutor::doUnion() {
    if (leftResult_ == nullptr && rightResult_ == nullptr) {
        VLOG(3) << "Union no input.";
        onEmptyInputs();
        return;
    }

    if (leftResult_ == nullptr) {
        VLOG(3) << "Union has right result.";
        finishExecution(std::move(rightResult_));
        return;
    }

    if (rightResult_ == nullptr) {
        VLOG(3) << "Union has left result.";
        finishExecution(std::move(leftResult_));
        return;
    }

    Status status = checkSchema();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(status);
    }

    auto rsWriter = std::make_unique<RowSetWriter>(resultSchema_);
    rsWriter->addAll(leftResult_->data());
    rsWriter->addAll(rightResult_->data());
    auto outputs = std::make_unique<InterimResult>(std::move(rsWriter));
    finishExecution(std::move(outputs));
}

Status SetExecutor::checkSchema() {
    auto leftSchema = leftResult_->schema();
    auto rightSchema = rightResult_->schema();
    auto leftIter = leftSchema->begin();
    auto rightIter = rightSchema->begin();

    while (leftIter && rightIter) {
        auto *leftName = leftIter->getName();
        auto *rightName = rightIter->getName();
        if (std::strcmp(leftName, rightName) != 0) {
            return Status::Error("Field name not equal, [%s]:[%s]",
                    leftName, rightName);
        }

        if (leftIter->getType() != rightIter->getType()) {
            return Status::Error("Field type not equal, [%s]:[%s]",
                    leftName, rightName);
        }
         ++leftIter;
         ++rightIter;
    }

    if (leftIter || rightIter) {
        return Status::Error("Field count not match.");
    }

    resultSchema_ = std::move(leftSchema);

    return Status::OK();
}

void SetExecutor::doIntersect() {
}

void SetExecutor::doMinus() {
}

void SetExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}

void SetExecutor::finishExecution(std::unique_ptr<InterimResult> result) {
    if (onResult_) {
        onResult_(std::move(result));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
        if (result != nullptr) {
            auto rows = result->getRows();
            resp_->set_rows(std::move(rows));
        }
    }

    DCHECK(onFinish_);
    onFinish_();
}

void SetExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    UNUSED(result);
    LOG(FATAL) << "Set operation not support input.";
}

void SetExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        return;
    }

    resp = std::move(*resp_);
}
}  // namespace graph
}  // namespace nebula
