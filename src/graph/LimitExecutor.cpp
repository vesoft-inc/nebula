/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/LimitExecutor.h"

namespace nebula {
namespace graph {

LimitExecutor::LimitExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "limit") {
    sentence_ = static_cast<LimitSentence*>(sentence);
}


Status LimitExecutor::prepare() {
    offset_ = sentence_->offset();
    if (offset_ < 0) {
        return Status::SyntaxError("skip `%ld' is illegal", offset_);
    }
    count_ = sentence_->count();
    if (count_ < 0) {
        return Status::SyntaxError("count `%ld' is illegal", count_);
    }

    return Status::OK();
}


void LimitExecutor::execute() {
    if (inputs_ == nullptr) {
        onEmptyInputs();
        return;
    }

    colNames_ = inputs_->getColNames();
    if (!inputs_->hasData() || count_ == 0) {
        onEmptyInputs();
        return;
    }

    auto ret = inputs_->getRows();
    if (!ret.ok()) {
        LOG(ERROR) << "Get rows failed: " << ret.status();
        doError(std::move(ret).status());
        return;
    }
    auto inRows = std::move(ret).value();
    if (inRows.size() > static_cast<uint64_t>(offset_ + count_)) {
        rows_.resize(count_);
        rows_.assign(std::make_move_iterator(inRows.begin()) + offset_,
                     std::make_move_iterator(inRows.begin()) + offset_ + count_);
    } else if (inRows.size() > static_cast<uint64_t>(offset_) &&
                   inRows.size() <= static_cast<uint64_t>(offset_ + count_)) {
        rows_.resize(inRows.size() - offset_);
        rows_.assign(std::make_move_iterator(inRows.begin()) + offset_,
                     std::make_move_iterator(inRows.end()));
    }

    if (onResult_) {
        auto status = setupInterimResult();
        if (!status.ok()) {
            DCHECK(onError_);
            onError_(std::move(status).status());
            return;
        }
        onResult_(std::move(status).value());
    }

    doFinish(Executor::ProcessControl::kNext);
}


StatusOr<std::unique_ptr<InterimResult>> LimitExecutor::setupInterimResult() {
    auto result = std::make_unique<InterimResult>(std::move(colNames_));
    if (rows_.empty()) {
        return result;
    }

    auto rsWriter = std::make_unique<RowSetWriter>(inputs_->schema());
    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows_) {
        RowWriter writer(inputs_->schema());
        auto columns = row.get_columns();
        for (auto &column : columns) {
            switch (column.getType()) {
                case cpp2::ColumnValue::Type::id:
                    writer << column.get_id();
                    break;
                case Type::integer:
                    writer << column.get_integer();
                    break;
                case Type::double_precision:
                    writer << column.get_double_precision();
                    break;
                case Type::bool_val:
                    writer << column.get_bool_val();
                    break;
                case Type::str:
                    writer << column.get_str();
                    break;
                case cpp2::ColumnValue::Type::timestamp:
                    writer << column.get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Not Support type: " << column.getType();
                    return Status::Error("Not Support type: %d", column.getType());
            }
        }
        rsWriter->addRow(writer);
    }

    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return result;
}


void LimitExecutor::onEmptyInputs() {
    if (onResult_) {
        auto result = std::make_unique<InterimResult>(std::move(colNames_));
        onResult_(std::move(result));
    }
    doFinish(Executor::ProcessControl::kNext);
}


void LimitExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp.set_column_names(std::move(colNames_));

    if (rows_.empty()) {
        return;
    }
    resp.set_rows(std::move(rows_));
}
}   // namespace graph
}   // namespace nebula
