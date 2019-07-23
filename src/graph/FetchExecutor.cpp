/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "FetchExecutor.h"

namespace nebula {
namespace graph {

Status FetchExecutor::prepareYield() {
    Status status = Status::OK();

    do {
        if (yieldClause_ == nullptr) {
            setupColumns();
        } else {
            yields_ = yieldClause_->columns();
            // TODO 'distinct' could always pushdown in fetch.
            distinct_ = yieldClause_->isDistinct();
        }

        for (auto *col : yields_) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                return status;
            }
            if (col->alias() == nullptr) {
                resultColNames_.emplace_back(col->expr()->toString());
            } else {
                resultColNames_.emplace_back(*col->alias());
            }
        }

        if (expCtx_->hasSrcTagProp() || expCtx_->hasDstTagProp()) {
            status = Status::SyntaxError(
                    "Only support form of alias.prop in fetch sentence.");
            break;
        }

        auto aliasProps = expCtx_->aliasProps();
        for (auto pair : aliasProps) {
            if (pair.first != *labelName_) {
                status = Status::SyntaxError(
                    "[%s.%s] tag not declared in %s.",
                    pair.first.c_str(), pair.second.c_str(), (*labelName_).c_str());
                break;
            }
        }
    } while (false);

    return status;
}

void FetchExecutor::setupColumns() {
    DCHECK_NOTNULL(labelSchema_);
    auto iter = labelSchema_->begin();
    if (yieldColsHolder_ == nullptr) {
        yieldColsHolder_ = std::make_unique<YieldColumns>();
    }
    while (iter) {
        auto *ref = new std::string("");
        auto *alias = new std::string(*labelName_);
        auto *prop = iter->getName();
        Expression *expr =
            new AliasPropertyExpression(ref, alias, new std::string(prop));
        YieldColumn *column = new YieldColumn(expr);
        yieldColsHolder_->addColumn(column);
        yields_.emplace_back(column);
        ++iter;
    }
}

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
            case WhichVariant::INT64_VAR:
                // all integers in InterimResult are regarded as type of VID
                type = SupportedType::VID;
                break;
            case WhichVariant::DOUBLE_VAR:
                type = SupportedType::DOUBLE;
                break;
            case WhichVariant::BOOL_VAR:
                type = SupportedType::BOOL;
                break;
            case WhichVariant::STRING_VAR:
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
