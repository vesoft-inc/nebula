/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/LimitExecutor.h"

namespace nebula {
namespace graph {

LimitExecutor::LimitExecutor(Sentence *sentence, ExecutionContext *ectx) : TraverseExecutor(ectx) {
    sentence_ = static_cast<LimitSentence*>(sentence);
}


Status LimitExecutor::prepare() {
    skip_ = sentence_->skip();
    if (skip_ < 0) {
        return Status::Error("skip `%ld' is illegal", skip_);
    }
    count_ = sentence_->count();
    if (count_ < 0) {
        return Status::Error("count `%ld' is illegal", count_);
    }

    return Status::OK();
}


void LimitExecutor::execute() {
    FLOG_INFO("Executing Limit: %s", sentence_->toString().c_str());
    if (inputs_ == nullptr) {
        DCHECK(onFinish_);
        onFinish_();
        return;
    }

    auto inRows = inputs_->getRows();
    if (inRows.size() > (skip_ + count_)) {
        rows_.resize(count_);
        rows_.assign(inRows.begin() + skip_, inRows.begin() + skip_ + count_);
    } else if (inRows.size() > skip_ && inRows.size() <= (skip_ + count_)) {
        rows_.resize(inRows.size() - skip_);
        rows_.assign(inRows.begin() + skip_, inRows.end());
    } else {
        rows_.resize(0);
    }

    if (onResult_) {
        auto output = setupInterimResult();
        onResult_(std::move(output));
    }

    DCHECK(onFinish_);
    onFinish_();
}


void LimitExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    if (result == nullptr) {
        return;
    }
    inputs_ = std::move(result);
}


std::unique_ptr<InterimResult> LimitExecutor::setupInterimResult() {
    if (rows_.empty()) {
        return nullptr;
    }

    auto rsWriter = std::make_unique<RowSetWriter>(inputs_->schema());
    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows_) {
        RowWriter writer(inputs_->schema());
        auto columns = row.get_columns();
        for (auto &column : columns) {
            switch (column.getType()) {
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
                default:
                    LOG(FATAL) << "Not Support: " << column.getType();
            }
        }
        rsWriter->addRow(writer);
    }

    return std::make_unique<InterimResult>(std::move(rsWriter));
}


void LimitExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (rows_.empty()) {
        return;
    }

    std::vector<std::string> columnNames;
    columnNames.reserve(inputs_->schema()->getNumFields());
    auto field = inputs_->schema()->begin();
    while (field) {
        columnNames.emplace_back(field->getName());
        ++field;
    }
    resp.set_column_names(std::move(columnNames));
    resp.set_rows(rows_);
}
}   // namespace graph
}   // namespace nebula
