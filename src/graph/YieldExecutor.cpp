/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/YieldExecutor.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace graph {

YieldExecutor::YieldExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<YieldSentence*>(sentence);
}


Status YieldExecutor::prepare() {
    Status status = Status::OK();
    do {
        status = prepareYield();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}

Status YieldExecutor::prepareYield() {
    yields_ = sentence_->columns();
    for (auto *col : yields_) {
        col->expr()->setContext(expCtx_.get());
        auto status = col->expr()->prepare();
        if (!status.ok()) {
            return status;
        }
        if (col->alias() == nullptr) {
            resultColNames_.emplace_back(col->expr()->toString());
        } else {
            resultColNames_.emplace_back(*col->alias());
        }
    }

    if (expCtx_->hasSrcTagProp()
            || expCtx_->hasDstTagProp()
            || expCtx_->hasEdgeProp()) {
        return Status::SyntaxError(
                    "Only support input and variable in yield sentence.");
    }

    if (expCtx_->hasVariableProp() && expCtx_->hasInputProp()) {
        return Status::Error("Not support both input and variable.");
    }

    if (expCtx_->hasVariableProp()) {
        auto &variables = expCtx_->variables();
        if (variables.size() > 1) {
            return Status::Error("Only one variable allowed to use");
        }

        varname_ = *variables.begin();
    }
    return Status::OK();
}

Status YieldExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    if (clause != nullptr) {
        filter_ = clause->filter();
    }
    return Status::OK();
}

void YieldExecutor::execute() {
    Status status = Status::OK();
    if (expCtx_->hasVariableProp() || expCtx_->hasInputProp()) {
        status = executeInputs();
    } else {
        status = executeConstant();
    }
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
}

Status YieldExecutor::executeInputs() {
    const auto *inputs = inputs_.get();

    if (expCtx_->hasVariableProp()) {
        bool existing = false;
        inputs = ectx()->variableHolder()->get(varname_, &existing);
        if (inputs == nullptr && !existing) {
            return Status::Error("Variable `%s' not defined", varname_.c_str());
        }
    }
    // No error happened, but we are having empty inputs
    if (inputs == nullptr) {
        return Status::OK();
    }

    auto outputSchema = std::make_shared<SchemaWriter>();
    auto status = getOutputSchema(inputs, outputSchema.get());
    if (!status.ok()) {
        return status;
    }

    auto rsWriter = std::make_unique<RowSetWriter>(outputSchema);
    auto visitor = [&] (const RowReader *reader) -> Status {
        auto &getters = expCtx_->getters();
        getters.getVariableProp = [&] (const std::string &prop) {
            return Collector::getProp(inputs->schema().get(), prop, reader);
        };
        getters.getInputProp = [&] (const std::string &prop) {
            return Collector::getProp(inputs->schema().get(), prop, reader);
        };
        if (filter_ != nullptr) {
            auto val = filter_->eval();
            if (!Expression::asBool(val)) {
                return Status::OK();
            }
        }

        auto writer = std::make_unique<RowWriter>(outputSchema);
        for (auto col : yields_) {
            auto *expr = col->expr();
            auto value = expr->eval();
            Collector::collect(value, writer.get());
        }
        rsWriter->addRow(*writer);
        return Status::OK();
    };
    inputs->applyTo(visitor);

    finishExecution(std::move(rsWriter));
    return Status::OK();
}

Status YieldExecutor::getOutputSchema(const InterimResult *inputs,
                                      SchemaWriter *outputSchema) const {
    if (expCtx_ == nullptr || resultColNames_.empty()) {
        return Status::Error("No result columns declared.");
    }

    auto *inputSchema = inputs->schema().get();
    auto varProps = expCtx_->variableProps();
    for (auto &pair : varProps) {
        if (inputSchema->getFieldIndex(pair.second) < 0) {
            return Status::Error(
                "column `%s' not exist in variable.", pair.second.c_str());
        }
    }
    auto inProps = expCtx_->inputProps();
    for (auto &prop : inProps) {
        if (inputSchema->getFieldIndex(prop) < 0) {
            return Status::Error(
                "column `%s' not exist in input.", prop.c_str());
        }
    }

    std::vector<VariantType> record;
    record.reserve(yields_.size());
    auto visitor = [&] (const RowReader *reader) -> Status {
        auto &getters = expCtx_->getters();
        getters.getVariableProp = [&] (const std::string &prop) {
            return Collector::getProp(inputSchema, prop, reader);
        };
        getters.getInputProp = [&] (const std::string &prop) {
            return Collector::getProp(inputSchema, prop, reader);
        };
        for (auto *column : yields_) {
            auto *expr = column->expr();
            auto value = expr->eval();
            record.emplace_back(std::move(value));
        }
        return Status::OK();
    };
    inputs->applyTo(visitor, 1);

    Collector::getSchema(record, resultColNames_, outputSchema);
    return Status::OK();
}

Status YieldExecutor::executeConstant() {
    auto size = yields_.size();
    std::vector<VariantType> values;
    values.reserve(size);

    for (auto *col : yields_) {
        auto expr = col->expr();
        auto v = expr->eval();
        if (!v.ok()) {
            onError_(v.status());
            return;
        }
        values.emplace_back(v.value());
    }

    auto outputSchema = std::make_shared<SchemaWriter>();
    Collector::getSchema(values, resultColNames_, outputSchema.get());

    RowWriter writer(std::move(outputSchema));
    auto rsWriter = std::make_unique<RowSetWriter>(outputSchema);
    for (auto col : values) {
        Collector::collect(col, &writer);
    }
    rsWriter->addRow(writer);
    finishExecution(std::move(rsWriter));
    return Status::OK();
}

void YieldExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
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

void YieldExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    inputs_ = std::move(result);
}

void YieldExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    CHECK(resp_ != nullptr);
    resp = std::move(*resp_);
}
}   // namespace graph
}   // namespace nebula
