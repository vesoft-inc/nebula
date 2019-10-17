/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/YieldExecutor.h"

namespace nebula {
namespace graph {

YieldExecutor::YieldExecutor(Sentence *sentence, ExecutionContext *ectx)
    : Executor(ectx) {
    sentence_ = static_cast<YieldSentence*>(sentence);
}


Status YieldExecutor::prepare() {
    // TODO(dutor) To make sure that all field expressions are simple ones,
    // i.e. could be evaluated instantly.
    yields_ = sentence_->columns();
    auto status = Status::OK();
    for (auto *col : yields_) {
        status = col->expr()->prepare();
        if (!status.ok()) {
            break;
        }
    }
    return status;
}


void YieldExecutor::execute() {
    auto size = yields_.size();
    std::vector<VariantType> values;
    values.reserve(size);

    using nebula::cpp2::SupportedType;
    std::vector<SupportedType> colTypes;
    for (auto *col : yields_) {
        auto expr = col->expr();
        auto v = expr->eval();
        if (!v.ok()) {
            onError_(v.status());
            return;
        }
        if (col->expr()->isTypeCastingExpression()) {
            auto exprPtr = static_cast<TypeCastingExpression *>(col->expr());
            colTypes.emplace_back(ColumnTypeToSupportedType(exprPtr->getType()));
        } else {
            switch (v.value().which()) {
                case VAR_INT64:
                    colTypes.emplace_back(SupportedType::INT);
                    break;
                case VAR_DOUBLE:
                    colTypes.emplace_back(SupportedType::DOUBLE);
                    break;
                case VAR_BOOL:
                    colTypes.emplace_back(SupportedType::BOOL);
                    break;
                case VAR_STR:
                    colTypes.emplace_back(SupportedType::STRING);
                    break;
                default:
                    LOG(FATAL) << "Unknown VariantType: " << v.value().which();
            }
        }
        values.emplace_back(v.value());
    }

    std::vector<cpp2::RowValue> rows;
    rows.emplace_back();
    std::vector<cpp2::ColumnValue> row;
    for (auto i = 0u; i < size; i++) {
        row.emplace_back();
        auto &value = values[i];
        /**
         * TODO(dutor)
         *  We are having so many tedious, duplicated type conversion codes.
         *  We shall reuse the conversion code and try to reduce conversion itself.
         */
        switch (colTypes[i]) {
            case SupportedType::INT:
                row.back().set_integer(boost::get<int64_t>(value));
                break;
            case SupportedType::DOUBLE:
                row.back().set_double_precision(boost::get<double>(value));
                break;
            case SupportedType::BOOL:
                row.back().set_bool_val(boost::get<bool>(value));
                break;
            case SupportedType::STRING:
                row.back().set_str(boost::get<std::string>(value));
                break;
            case SupportedType::TIMESTAMP:
                row.back().set_timestamp(boost::get<int64_t>(value));
                break;
            default:
                LOG(FATAL) << "Unknown SupportedType: " << static_cast<int32_t>(colTypes[i]);
        }
    }
    rows.back().set_columns(std::move(row));

    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    resp_->set_column_names(getResultColumnNames());
    resp_->set_rows(std::move(rows));

    onFinish_();
}


void YieldExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    CHECK(resp_ != nullptr);
    resp = std::move(*resp_);
}


std::vector<std::string> YieldExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yields_.size());
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->expr()->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}

}   // namespace graph
}   // namespace nebula
