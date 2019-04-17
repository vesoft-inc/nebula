/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    std::vector<VariantType> values(size);

    auto eval = [] (auto *col) {
        return col->expr()->eval();
    };
    std::transform(yields_.begin(), yields_.end(), values.begin(), eval);

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
        switch (value.which()) {
            case 0:
                row.back().set_integer(boost::get<int64_t>(value));
                break;
            case 1:
                row.back().set_double_precision(boost::get<double>(value));
                break;
            case 2:
                row.back().set_bool_val(boost::get<bool>(value));
                break;
            case 3:
                row.back().set_str(boost::get<std::string>(value));
                break;
            default:
                LOG(FATAL) << "Unknown VariantType: " << value.which();
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
