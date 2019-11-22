/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/OrderByExecutor.h"

namespace nebula {
namespace graph {
namespace cpp2 {

bool ColumnValue::operator < (const ColumnValue& rhs) const {
    DCHECK_EQ(type_, rhs.type_);
    auto& lhs = *this;
    switch (lhs.type_) {
        case Type::bool_val:
        {
            return lhs.value_.bool_val < rhs.value_.bool_val;
        }
        case Type::integer:
        {
            return lhs.value_.integer < rhs.value_.integer;
        }
        case Type::id:
        {
            return lhs.value_.id < rhs.value_.id;
        }
        case Type::single_precision:
        {
            return lhs.value_.single_precision < rhs.value_.single_precision;
        }
        case Type::double_precision:
        {
            return lhs.value_.double_precision < rhs.value_.double_precision;
        }
        case Type::str:
        {
            return lhs.value_.str < rhs.value_.str;
        }
        case Type::timestamp:
        {
            return lhs.value_.timestamp < rhs.value_.timestamp;
        }
        case Type::year:
        {
            return lhs.value_.year < rhs.value_.year;
        }
        case Type::month:
        {
            return lhs.value_.month < rhs.value_.month;
        }
        case Type::date:
        {
            return lhs.value_.date < rhs.value_.date;
        }
        case Type::datetime:
        {
            return lhs.value_.datetime < rhs.value_.datetime;
        }
        default:
        {
            return false;
        }
    }
    return false;
}
}  // namespace cpp2

OrderByExecutor::OrderByExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<OrderBySentence*>(sentence);
}

Status OrderByExecutor::prepare() {
    return Status::OK();
}

void OrderByExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    if (result == nullptr) {
        return;
    }
    DCHECK(sentence_ != nullptr);
    inputs_ = std::move(result);
    colNames_ = inputs_->getColNames();
    auto ret = inputs_->getRows();
    if (!ret.ok()) {
        return;
    }
    rows_ = std::move(ret).value();
}

void OrderByExecutor::execute() {
    DCHECK(onFinish_);
    DCHECK(onError_);
    FLOG_INFO("Executing Order By: %s", sentence_->toString().c_str());
    auto status = beforeExecute();
    if (!status.ok()) {
        LOG(ERROR) << "Error happened before execute: " << status.toString();
        onError_(std::move(status));
        return;
    }

    auto comparator = [this] (cpp2::RowValue& lhs, cpp2::RowValue& rhs) {
        const auto &lhsColumns = lhs.get_columns();
        const auto &rhsColumns = rhs.get_columns();
        for (auto &factor : this->sortFactors_) {
            auto fieldIndex = factor.first;
            auto orderType = factor.second;
            if (lhsColumns[fieldIndex] == rhsColumns[fieldIndex]) {
                continue;
            }

            if (orderType == OrderFactor::OrderType::ASCEND) {
                return lhsColumns[fieldIndex] < rhsColumns[fieldIndex];
            } else if (orderType == OrderFactor::OrderType::DESCEND) {
                return lhsColumns[fieldIndex] > rhsColumns[fieldIndex];
            } else {
                LOG(FATAL) << "Unkown Order Type: " << orderType;
            }
        }
        return false;
    };

    if (!sortFactors_.empty()) {
        std::sort(rows_.begin(), rows_.end(), comparator);
    }

    if (onResult_) {
        onResult_(setupInterimResult());
    }
    onFinish_(Executor::ProcessControl::kNext);
}

Status OrderByExecutor::beforeExecute() {
    if (inputs_ == nullptr || !inputs_->hasData()) {
        return Status::OK();
    }

    auto schema = inputs_->schema();
    auto factors = sentence_->factors();
    sortFactors_.reserve(factors.size());
    for (auto &factor : factors) {
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        folly::StringPiece field = *(expr->prop());
        auto fieldIndex = schema->getFieldIndex(field);
        if (fieldIndex == -1) {
            return Status::Error("Field (%s) not exist in input schema.", field.str().c_str());
        }
        auto pair = std::make_pair(schema->getFieldIndex(field), factor->orderType());
        sortFactors_.emplace_back(std::move(pair));
    }
    return Status::OK();
}

std::unique_ptr<InterimResult> OrderByExecutor::setupInterimResult() {
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
                    LOG(FATAL) << "Not Support: " << column.getType();
            }
        }
        rsWriter->addRow(writer);
    }

    result->setInterim(std::move(rsWriter));
    return result;
}

void OrderByExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (rows_.empty()) {
        return;
    }

    resp.set_column_names(std::move(colNames_));
    resp.set_rows(std::move(rows_));
}

}  // namespace graph
}  // namespace nebula
