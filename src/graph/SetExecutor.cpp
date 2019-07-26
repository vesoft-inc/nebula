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

    auto onFinish = [] () {
        return;
    };

    {
        futures_.emplace_back(leftP_.getFuture());
        auto onResult = [this] (std::unique_ptr<InterimResult> result) {
            this->leftResult_ = std::move(result);
            VLOG(3) << "Left result set.";
            leftP_.setValue();
        };
        auto onError = [this] (Status s) {
            VLOG(3) << "Left error:" << s.toString();
            leftS_ = std::move(s);
            leftP_.setValue();
        };
        left_->setOnResult(onResult);
        left_->setOnFinish(onFinish);
        left_->setOnError(onError);
    }
    {
        futures_.emplace_back(rightP_.getFuture());
        auto onResult = [this] (std::unique_ptr<InterimResult> result) {
            this->rightResult_ = std::move(result);
            VLOG(3) << "Right result set.";
            rightP_.setValue();
        };
        auto onError = [this] (Status s) {
            VLOG(3) << "Right error: " << s.toString();
            rightS_ = std::move(s);
            rightP_.setValue();
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
        if (!leftS_.ok() || !rightS_.ok()) {
            std::string msg;
            msg += "left: ";
            msg += leftS_.toString();
            msg += " right: ";
            msg += rightS_.toString();
            onError_(Status::Error(msg));
            return;
        }

        if (leftResult_ == nullptr && rightResult_ == nullptr) {
            VLOG(3) << "Set op no input.";
            onEmptyInputs();
            return;
        }
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
    VLOG(3) << "Do Union.";
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
        return;
    }

    auto leftRows = leftResult_->getRows();
    auto rightRows = rightResult_->getRows();
    if (!castingMap_.empty()) {
        auto stat = doCasting(rightRows);
        if (!stat.ok()) {
            DCHECK(onError_);
            onError_(status);
            return;
        }
    }

    std::vector<cpp2::RowValue> rows;
    if (sentence_->distinct()) {
        rows = doDistinct(leftRows, rightRows);
    } else {
        leftRows.insert(leftRows.end(), rightRows.begin(), rightRows.end());
        rows = std::move(leftRows);
    }

    finishExecution(std::move(rows));
    return;
}

Status SetExecutor::checkSchema() {
    auto leftSchema = leftResult_->schema();
    auto rightSchema = rightResult_->schema();
    auto leftIter = leftSchema->begin();
    auto rightIter = rightSchema->begin();

    auto index = 0u;
    while (leftIter && rightIter) {
        auto *colName = rightIter->getName();
        if (leftIter->getType() != rightIter->getType()) {
            castingMap_.emplace(index, leftIter->getType());
        }

        colNames_.emplace_back(std::string(colName));

        ++index;
        ++leftIter;
        ++rightIter;
    }

    if (leftIter || rightIter) {
        return Status::Error("Field count not match.");
    }

    resultSchema_ = std::move(leftSchema);

    return Status::OK();
}

Status SetExecutor::doCasting(std::vector<cpp2::RowValue> &rows) const {
    for (auto &row : rows) {
       auto cols = row.get_columns();
       for (auto &pair : castingMap_) {
           auto stat =
               InterimResult::castTo(&cols[pair.first], pair.second.get_type());
            if (!stat.ok()) {
                return stat;
            }
       }
       row.set_columns(std::move(cols));
    }

    return Status::OK();
}


std::vector<cpp2::RowValue> SetExecutor::doDistinct(
        std::vector<cpp2::RowValue> &leftRows,
        std::vector<cpp2::RowValue> &rightRows) const {
    std::vector<cpp2::RowValue> rows;
    for (auto &lr : leftRows) {
        auto iter = rows.begin();
        while (iter != rows.end()) {
            if (lr == *iter) {
                break;
            }
            ++iter;
        }
        if (iter == rows.end()) {
            rows.emplace_back(std::move(lr));
        }
    }

    for (auto &rr : rightRows) {
        auto iter = rows.begin();
        while (iter != rows.end()) {
            if (rr == *iter) {
                break;
            }
            ++iter;
        }
        if (iter == rows.end()) {
            rows.emplace_back(std::move(rr));
        }
    }

    return rows;
}

void SetExecutor::doIntersect() {
    VLOG(3) << "Do InterSect.";
    if (leftResult_ == nullptr || rightResult_ == nullptr) {
        VLOG(3) << "No intersect.";
        onEmptyInputs();
        return;
    }

    Status status = checkSchema();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(status);
        return;
    }

    auto leftRows = leftResult_->getRows();
    auto rightRows = rightResult_->getRows();
    if (!castingMap_.empty()) {
        Status stat = doCasting(rightRows);
        if (!stat.ok()) {
            DCHECK(onError_);
            onError_(status);
            return;
        }
    }

    std::vector<cpp2::RowValue> rows;
    for (auto &lr : leftRows) {
        for (auto &rr : rightRows) {
            if (rr == lr) {
                rows.emplace_back(std::move(rr));
                break;
            }
        }
    }

    finishExecution(std::move(rows));
    return;
}

void SetExecutor::doMinus() {
    VLOG(3) << "Do Minus.";
    if (leftResult_ == nullptr) {
        VLOG(3) << "Minus has only right result.";
        onEmptyInputs();
        return;
    }

    if (rightResult_ == nullptr) {
        VLOG(3) << "Minus has left result.";
        finishExecution(std::move(leftResult_));
        return;
    }

    Status status = checkSchema();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(status);
        return;
    }

    auto leftRows = leftResult_->getRows();
    auto rightRows = rightResult_->getRows();
    if (!castingMap_.empty()) {
        Status stat = doCasting(rightRows);
        if (!stat.ok()) {
            DCHECK(onError_);
            onError_(status);
            return;
        }
    }

    for (auto &rr : rightRows) {
        for (auto iter = leftRows.begin(); iter < leftRows.end();) {
            if (rr == *iter) {
                iter = leftRows.erase(iter);
            } else {
                ++iter;
            }
        }
    }

    finishExecution(std::move(leftRows));
    return;
}

void SetExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    DCHECK(onFinish_);
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

void SetExecutor::finishExecution(std::vector<cpp2::RowValue> rows) {
    if (onResult_) {
        auto outputs = InterimResult::getInterim(resultSchema_, rows);
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
        resp_->set_rows(std::move(rows));
    }
    DCHECK(onFinish_);
    onFinish_();
}

void SetExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    // Feed input for set operator is an act of reservation.
    UNUSED(result);
    LOG(FATAL) << "Set operation not support input yet.";
}

void SetExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        return;
    }

    resp = std::move(*resp_);
}
}  // namespace graph
}  // namespace nebula
