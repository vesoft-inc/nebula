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
    auto onFinish = [] (Executor::ProcessControl ctr) {
        UNUSED(ctr);
        return;
    };

    futures_.emplace_back(leftP_.getFuture());
    auto onResult = [this] (std::unique_ptr<InterimResult> result) {
        DCHECK(result != nullptr);
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
    auto onFinish = [] (Executor::ProcessControl ctr) {
        UNUSED(ctr);
        return;
    };

    futures_.emplace_back(rightP_.getFuture());
    auto onResult = [this] (std::unique_ptr<InterimResult> result) {
        DCHECK(result != nullptr);
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
            msg += "lhs has error: ";
            msg += leftS_.toString();
            msg += " rhs has error: ";
            msg += rightS_.toString();
            onError_(Status::Error(msg));
            return;
        }

        if (leftResult_ == nullptr || rightResult_ == nullptr) {
            // Should not reach here.
            LOG(ERROR) << "Get null input.";
            onError_(Status::Error("Get null input."));
            return;
        }

        colNames_ = leftResult_->getColNames();
        // If the column count not match, we will not do set op.
        if (colNames_.size() != rightResult_->getColNames().size()) {
            std::string err = "Field count not match.";
            LOG(ERROR) << err;
            onError_(Status::Error(std::move(err)));
            return;
        }
        if (!leftResult_->hasData() && !rightResult_->hasData()) {
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
    if (!leftResult_->hasData()) {
        VLOG(3) << "Union has right result.";
        rightResult_->setColNames(std::move(colNames_));
        finishExecution(std::move(rightResult_));
        return;
    }

    if (!rightResult_->hasData()) {
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

    auto ret = leftResult_->getRows();
    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(std::move(ret).status());
        return;
    }
    auto leftRows = std::move(ret).value();

    ret = rightResult_->getRows();
    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(std::move(ret).status());
        return;
    }
    auto rightRows = std::move(ret).value();

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
        if (leftIter->getType() != rightIter->getType()) {
            // Implicit type casting would happen if the type do no match.
            // If type casting failed. the whole statement would fail.
            castingMap_.emplace_back(index, leftIter->getType());
        }

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
    if (!leftResult_->hasData() || !rightResult_->hasData()) {
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

    auto ret = leftResult_->getRows();
    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(std::move(ret).status());
        return;
    }
    auto leftRows = std::move(ret).value();

    ret = rightResult_->getRows();
    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(std::move(ret).status());
        return;
    }
    auto rightRows = std::move(ret).value();

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
    if (!leftResult_->hasData()) {
        VLOG(3) << "Minus has only right result.";
        onEmptyInputs();
        return;
    }

    if (!rightResult_->hasData()) {
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

    auto ret = leftResult_->getRows();
    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(std::move(ret).status());
        return;
    }
    auto leftRows = std::move(ret).value();

    ret = rightResult_->getRows();
    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(std::move(ret).status());
        return;
    }
    auto rightRows = std::move(ret).value();

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
        auto result = std::make_unique<InterimResult>(std::move(colNames_));
        onResult_(std::move(result));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
    }
    DCHECK(onFinish_);
    onFinish_(Executor::ProcessControl::kNext);
}

void SetExecutor::finishExecution(std::unique_ptr<InterimResult> result) {
    if (result == nullptr) {
        result = std::make_unique<InterimResult>(std::move(colNames_));
    }
    if (onResult_) {
        onResult_(std::move(result));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        auto colNames = result->getColNames();
        resp_->set_column_names(std::move(colNames));
        if (result->hasData()) {
            auto ret = result->getRows();
            if (!ret.ok()) {
                DCHECK(onError_);
                onError_(std::move(ret).status());
                return;
            }
            resp_->set_rows(std::move(ret).value());
        }
    }

    DCHECK(onFinish_);
    onFinish_(Executor::ProcessControl::kNext);
}

void SetExecutor::finishExecution(std::vector<cpp2::RowValue> rows) {
    if (onResult_) {
        auto ret = InterimResult::getInterim(resultSchema_, rows);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Interim result failed.";
            onError_(std::move(ret).status());
        }
        onResult_(std::move(ret).value());
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
        resp_->set_rows(std::move(rows));
    }
    DCHECK(onFinish_);
    onFinish_(Executor::ProcessControl::kNext);
}

void SetExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    // Feed input for set operator is an act of reservation.
    UNUSED(result);
    LOG(FATAL) << "Set operation not support input yet.";
}

void SetExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
        return;
    }

    resp = std::move(*resp_);
}
}  // namespace graph
}  // namespace nebula
