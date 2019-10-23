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
    FLOG_INFO("Executing Limit: %s", sentence_->toString().c_str());
    if (inputs_ == nullptr || count_ == 0) {
        DCHECK(onFinish_);
        onFinish_();
        return;
    }

    auto ret = inputs_->getRows();
    if (!ret.ok()) {
        DCHECK(onFinish_);
        onFinish_();
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
                    LOG(FATAL) << "Not Support: " << column.getType();
            }
        }
        rsWriter->addRow(writer);
    }

    auto result = std::make_unique<InterimResult>(getResultColumnNames());
    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return result;
}


std::vector<std::string> LimitExecutor::getResultColumnNames() const {
    std::vector<std::string> columnNames;
    columnNames.reserve(inputs_->schema()->getNumFields());
    auto field = inputs_->schema()->begin();
    while (field) {
        columnNames.emplace_back(field->getName());
        ++field;
    }
    return columnNames;
}


void LimitExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp.set_column_names(getResultColumnNames());
    if (rows_.empty()) {
        return;
    }

    resp.set_rows(std::move(rows_));
}
}   // namespace graph
}   // namespace nebula
