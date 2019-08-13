/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/SetExecutor.h"

namespace nebula {
namespace graph {
namespace cpp2 {
bool RowValue::operator<(const RowValue& rhs) const {
    auto &lhs = *this;
    return (lhs.columns < rhs.columns);
}
}

SetExecutor::SetExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<SetSentence*>(sentence);
}

Status SetExecutor::prepare() {
    left_ = makeTraverseExecutor(sentence_->left());
    right_ = makeTraverseExecutor(sentence_->right());
    DCHECK(left_ != nullptr);
    DCHECK(right_ != nullptr);

    setLeft();
    setRight();

    auto status = left_->prepare();
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

void SetExecutor::setLeft() {
    auto onFinish = [] () {
        return;
    };

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

void SetExecutor::setRight() {
    auto onFinish = [] () {
        return;
    };

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

void SetExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        onError_(std::move(status));
        return;
    }

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

        // Set op share the same priority, they would execute sequentially.
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
                LOG(FATAL) << "Unknown operator: " << sentence_->op();
        }
    };
    folly::collectAll(futures_).via(runner).thenValue(cb);
}

void SetExecutor::doUnion() {
    VLOG(3) << "Do Union.";
    if (leftResult_ == nullptr) {
        VLOG(3) << "Union has right result.";
        getResultCols(rightResult_);
        finishExecution(std::move(rightResult_));
        return;
    }

    if (rightResult_ == nullptr) {
        VLOG(3) << "Union has left result.";
        getResultCols(leftResult_);
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

    leftRows.insert(leftRows.end(),
                    std::make_move_iterator(rightRows.begin()),
                    std::make_move_iterator(rightRows.end()));
    if (sentence_->distinct()) {
        doDistinct(leftRows);
    }

    finishExecution(std::move(leftRows));
    return;
}

Status SetExecutor::checkSchema() {
    auto leftSchema = leftResult_->schema();
    auto rightSchema = rightResult_->schema();
    auto leftIter = leftSchema->begin();
    auto rightIter = rightSchema->begin();

    auto index = 0u;
    while (leftIter && rightIter) {
        auto *colName = leftIter->getName();
        if (leftIter->getType() != rightIter->getType()) {
            // Implicit type casting would happen if the type do no match.
            // If type casting failed. the whole statement would fail.
            castingMap_.emplace_back(index, leftIter->getType());
        }

        colNames_.emplace_back(colName);

        ++index;
        ++leftIter;
        ++rightIter;
    }

    if (leftIter || rightIter) {
        // If the column count not match, we will not do set op.
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


void SetExecutor::doDistinct(std::vector<cpp2::RowValue> &rows) const {
    std::sort(rows.begin(), rows.end());
    auto it = std::unique(rows.begin(), rows.end());
    rows.erase(it, rows.end());
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
        getResultCols(leftResult_);
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

void SetExecutor::getResultCols(std::unique_ptr<InterimResult> &result) {
    auto schema = result->schema();
    auto iter = schema->begin();
    while (iter) {
        auto *colName = iter->getName();
        colNames_.emplace_back(colName);
        ++iter;
    }
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
