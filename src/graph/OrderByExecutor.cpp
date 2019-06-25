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
            if (!(lhs.value_.bool_val == rhs.value_.bool_val)) {
                return lhs.value_.bool_val < rhs.value_.bool_val;
            }
            break;
        }
        case Type::integer:
        {
            if (!(lhs.value_.integer == rhs.value_.integer)) {
                return lhs.value_.integer < rhs.value_.integer;
            }
            break;
        }
        case Type::id:
        {
            if (!(lhs.value_.id == rhs.value_.id)) {
                return lhs.value_.id < rhs.value_.id;
            }
            break;
        }
        case Type::single_precision:
        {
            if (!(lhs.value_.single_precision == rhs.value_.single_precision)) {
                return lhs.value_.single_precision < rhs.value_.single_precision;
            }
            break;
        }
        case Type::double_precision:
        {
            if (!(lhs.value_.double_precision == rhs.value_.double_precision)) {
                return lhs.value_.double_precision < rhs.value_.double_precision;
            }
            break;
        }
        case Type::str:
        {
            if (!(lhs.value_.str == rhs.value_.str)) {
                return lhs.value_.str < rhs.value_.str;
            }
            break;
        }
        case Type::timestamp:
        {
            if (!(lhs.value_.timestamp == rhs.value_.timestamp)) {
                return lhs.value_.timestamp < rhs.value_.timestamp;
            }
            break;
        }
        case Type::year:
        {
            if (!(lhs.value_.year == rhs.value_.year)) {
                return lhs.value_.year < rhs.value_.year;
            }
            break;
        }
        case Type::month:
        {
            if (!(lhs.value_.month == rhs.value_.month)) {
                return lhs.value_.month < rhs.value_.month;
            }
            break;
        }
        case Type::date:
        {
            if (!(lhs.value_.date == rhs.value_.date)) {
                return lhs.value_.date < rhs.value_.date;
            }
            break;
        }
        case Type::datetime:
        {
            if (!(lhs.value_.datetime == rhs.value_.datetime)) {
                return lhs.value_.datetime < rhs.value_.datetime;
            }
            break;
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
    rows_ = inputs_->getRows();

    auto schema = inputs_->schema();
    auto factors = sentence_->factors();
    sortFactors_.reserve(factors.size());
    for (auto &factor : factors) {
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        folly::StringPiece field = *(expr->prop());
        auto fieldIndex = schema->getFieldIndex(field);
        if (fieldIndex == -1) {
            LOG(INFO) << "Field(" << field << ") not exist in input schema.";
            continue;
        }
        auto pair = std::make_pair(schema->getFieldIndex(field), factor->orderType());
        sortFactors_.emplace_back(std::move(pair));
    }
}

void OrderByExecutor::execute() {
    FLOG_INFO("Executing Order By: %s", sentence_->toString().c_str());
    auto comparator = [this] (cpp2::RowValue& lhs, cpp2::RowValue& rhs) {
        auto lhsColumns = lhs.get_columns();
        auto rhsColumns = rhs.get_columns();
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
        LOG(INFO) << "on result";
        onResult_(setupInterimResult());
    }
    DCHECK(onFinish_);
    onFinish_();
}

std::unique_ptr<InterimResult> OrderByExecutor::setupInterimResult() {
    if (rows_.empty()) {
        return nullptr;
    }

    auto schema = inputs_->schema();
    auto rsWriter = std::make_unique<RowSetWriter>(schema);
    using Type = cpp2::ColumnValue::Type;
    LOG(INFO) << rows_.size();
    for (auto &row : rows_) {
        RowWriter writer(schema);
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

void OrderByExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (rows_.empty()) {
        return;
    }

    auto schema = inputs_->schema();
    std::vector<std::string> columnNames;
    columnNames.reserve(schema->getNumFields());
    auto field = schema->begin();
    while (field) {
        columnNames.emplace_back(field->getName());
        ++field;
    }
    resp.set_column_names(std::move(columnNames));
    resp.set_rows(std::move(rows_));
}

}  // namespace graph
}  // namespace nebula
