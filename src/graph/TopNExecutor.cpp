/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/TopNExecutor.h"

namespace nebula {
namespace graph {
TopNExecutor::TopNExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "top_n") {
    sentence_ = static_cast<TopNSentence*>(sentence);
}

Status TopNExecutor::prepare() {
    return Status::OK();
}

void TopNExecutor::execute() {
    if (inputs_ == nullptr) {
        doFinish(Executor::ProcessControl::kNext);
        return;
    }

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    colNames_ = inputs_->getColNames();
    if (!inputs_->hasData()) {
        doFinish(Executor::ProcessControl::kNext);
        return;
    }

    auto schema = inputs_->schema();
    auto factors = sentence_->factors();
    std::vector<int64_t> indexes;
    indexes.reserve(factors.size());
    for (auto &factor : factors) {
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        folly::StringPiece field = *(expr->prop());
        auto fieldIndex = schema->getFieldIndex(field);
        if (fieldIndex == -1) {
            doError(Status::Error("Field (%s) not exist in input schema.", field.str().c_str()));
            return;
        }
        indexes.emplace_back(fieldIndex);
    }

    auto limit = sentence_->limit();
    if (limit < 0) {
        doError(Status::Error("Limit(%ld) should be a positive integer.", limit));
        return;
    }
    auto rowRet = inputs_->getTopNRows(indexes, limit);
    if (!rowRet.ok()) {
        LOG(ERROR) << "Get rows failed: " << rowRet.status();
        doError(rowRet.status());
        return;
    }
    rows_ = std::move(rowRet).value();
    if (onResult_) {
        auto ret = setupInterimResult();
        if (!ret.ok()) {
            doError(std::move(ret).status());
            return;
        }
        onResult_(std::move(ret).value());
    }
    doFinish(Executor::ProcessControl::kNext);
}

StatusOr<std::unique_ptr<InterimResult>> TopNExecutor::setupInterimResult() {
    auto result = std::make_unique<InterimResult>(std::move(colNames_));
    if (rows_.empty()) {
        return result;
    }

    auto schema = inputs_->schema();
    auto rsWriter = std::make_unique<RowSetWriter>(schema);
    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows_) {
        RowWriter writer(schema);
        auto columns = row.get_columns();
        for (auto &column : columns) {
            switch (column.getType()) {
                case Type::id:
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
                case Type::timestamp:
                    writer << column.get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Not Support type: " << column.getType();
                    return Status::Error("Not Support type: %d", column.getType());
            }
        }
        rsWriter->addRow(writer);
    }

    result->setInterim(std::move(rsWriter));
    return result;
}

void TopNExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp.set_column_names(std::move(colNames_));

    if (rows_.empty()) {
        return;
    }
    resp.set_rows(std::move(rows_));
}
}  // namespace graph
}  // namespace nebula
