/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "FetchExecutor.h"

namespace nebula {
namespace graph {

void FetchExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void FetchExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}

void FetchExecutor::getOutputSchema(
        meta::SchemaProviderIf *schema,
        const RowReader *reader,
        SchemaWriter *outputSchema) const {
    if (expCtx_ == nullptr || resultColNames_.empty()) {
        return;
    }
    auto collector = std::make_unique<Collector>(schema);
    auto &getters = expCtx_->getters();
    getters.getAliasProp = [&] (const std::string&, const std::string &prop) {
        return collector->getProp(prop, reader);
    };
    std::vector<VariantType> record;
    for (auto *column : yields_) {
        auto *expr = column->expr();
        auto value = expr->eval();
        record.emplace_back(std::move(value));
    }

    using nebula::cpp2::SupportedType;
    for (auto index = 0u; index < record.size(); ++index) {
        SupportedType type;
        switch (record[index].which()) {
            case 0:
                // all integers in InterimResult are regarded as type of VID
                type = SupportedType::VID;
                break;
            case 1:
                type = SupportedType::DOUBLE;
                break;
            case 2:
                type = SupportedType::BOOL;
                break;
            case 3:
                type = SupportedType::STRING;
                break;
            default:
                LOG(FATAL) << "Unknown VariantType: " << record[index].which();
        }
        outputSchema->appendCol(resultColNames_[index], type);
    }
}

void FetchExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
    std::unique_ptr<InterimResult> outputs;
    if (rsWriter != nullptr) {
        outputs = std::make_unique<InterimResult>(std::move(rsWriter));
    }

    if (onResult_) {
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(resultColNames_));
        if (outputs != nullptr) {
            auto rows = outputs->getRows();
            resp_->set_rows(std::move(rows));
        }
    }
    DCHECK(onFinish_);
    onFinish_();
}
}  // namespace graph
}  // namespace nebula
