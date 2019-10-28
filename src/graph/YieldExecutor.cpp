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
    expCtx_ = std::make_unique<ExpressionContext>();
    do {
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}

Status YieldExecutor::prepareYield() {
    // this preparation depends on interim result,
    // it can only be called after getting results of the previous executor,
    // but if we can do the semantic analysis before execution,
    // then we can do the preparation before execution
    // TODO: make it possible that this preparation not depends on interim result
    auto *clause = sentence_->yield();
    yieldClauseWrapper_ = std::make_unique<YieldClauseWrapper>(clause);
    auto *varHolder = ectx()->variableHolder();
    auto status = yieldClauseWrapper_->prepare(inputs_.get(), varHolder, yields_);
    if (!status.ok()) {
        return status;
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

        // such as YIELD 1+1, it has not type in schema, the type from the eval()
        colTypes_.emplace_back(nebula::cpp2::SupportedType::UNKNOWN);
        if (col->expr()->isTypeCastingExpression()) {
            // type cast
            auto exprPtr = static_cast<TypeCastingExpression*>(col->expr());
            colTypes_.back() = ColumnTypeToSupportedType(exprPtr->getType());
        }
    }

    return Status::OK();
}

Status YieldExecutor::prepareWhere() {
    Status status;
    auto *clause = sentence_->where();
    if (clause != nullptr) {
        filter_ = clause->filter();
    }
    if (filter_ != nullptr) {
        filter_->setContext(expCtx_.get());
        status = filter_->prepare();
    }
    return status;
}

Status YieldExecutor::syntaxCheck() {
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
            return Status::Error("Only one variable allowed to use.");
        }

        varname_ = *variables.begin();
    }

    return Status::OK();
}

void YieldExecutor::execute() {
    FLOG_INFO("Executing YIELD: %s", sentence_->toString().c_str());
    Status status;
    do {
        status = beforeExecute();
        if (!status.ok()) {
            break;
        }
        if (expCtx_->hasVariableProp() || expCtx_->hasInputProp()) {
            status = executeInputs();
        } else {
            status = executeConstant();
        }
    } while (false);

    if (!status.ok()) {
        LOG(INFO) << status.toString();
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
}

Status YieldExecutor::beforeExecute() {
    Status status;
    do {
        status = prepareYield();
        if (!status.ok()) {
            break;
        }
        status = syntaxCheck();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status YieldExecutor::executeInputs() {
    const auto *inputs = inputs_.get();

    if (expCtx_->hasVariableProp()) {
        bool existing = false;
        inputs = ectx()->variableHolder()->get(varname_, &existing);
        if (inputs == nullptr && !existing) {
            return Status::Error("Variable `%s' not defined.", varname_.c_str());
        }
    }
    // No error happened, but we are having empty inputs
    if (inputs == nullptr) {
        finishExecution(nullptr);
        return Status::OK();
    }

    auto outputSchema = std::make_shared<SchemaWriter>();
    auto status = getOutputSchema(inputs, outputSchema.get());
    if (!status.ok()) {
        return status;
    }

    auto rsWriter = std::make_unique<RowSetWriter>(outputSchema);
    auto visitor =
        [inputs, &outputSchema, &rsWriter, &status, this] (const RowReader *reader) -> Status {
        auto &getters = expCtx_->getters();
        getters.getVariableProp = [inputs, reader] (const std::string &prop) {
            return Collector::getProp(inputs->schema().get(), prop, reader);
        };
        getters.getInputProp = [inputs, reader] (const std::string &prop) {
            return Collector::getProp(inputs->schema().get(), prop, reader);
        };
        if (filter_ != nullptr) {
            auto val = filter_->eval();
            if (!val.ok()) {
                return val.status();
            }
            if (!Expression::asBool(val.value())) {
                return Status::OK();
            }
        }

        auto writer = std::make_unique<RowWriter>(outputSchema);
        for (auto col : yields_) {
            auto *expr = col->expr();
            auto value = expr->eval();
            if (!value.ok()) {
                return value.status();
            }
            status = Collector::collect(value.value(), writer.get());
            if (!status.ok()) {
                return status;
            }
        }
        rsWriter->addRow(*writer);
        return Status::OK();
    };
    status = inputs->applyTo(visitor);
    if (!status.ok()) {
        return status;
    }

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
    auto varPropFind = std::find_if(varProps.begin(), varProps.end(),
            [inputSchema] (const auto &pair) {
                return inputSchema->getFieldIndex(pair.second) < 0;
            });
    if (varPropFind != varProps.end()) {
        return Status::Error(
            "column `%s' not exist in variable.", varPropFind->second.c_str());
    }

    auto inProps = expCtx_->inputProps();
    auto inPropFind = std::find_if(inProps.begin(), inProps.end(),
            [inputSchema] (const auto &prop) {
                return inputSchema->getFieldIndex(prop) < 0;
            });
    if (inPropFind != inProps.end()) {
        return Status::Error(
            "column `%s' not exist in input.", inPropFind->c_str());
    }

    std::vector<VariantType> record;
    record.reserve(yields_.size());
    auto visitor = [inputSchema, &record, this] (const RowReader *reader) -> Status {
        auto &getters = expCtx_->getters();
        getters.getVariableProp = [inputSchema, reader] (const std::string &prop) {
            return Collector::getProp(inputSchema, prop, reader);
        };
        getters.getInputProp = [inputSchema, reader] (const std::string &prop) {
            return Collector::getProp(inputSchema, prop, reader);
        };
        for (auto *column : yields_) {
            auto *expr = column->expr();
            auto value = expr->eval();
            if (!value.ok()) {
                return value.status();
            }
            record.emplace_back(std::move(value).value());
        }
        return Status::OK();
    };
    inputs->applyTo(visitor, 1);

    return Collector::getSchema(record, resultColNames_, colTypes_, outputSchema);
}

Status YieldExecutor::executeConstant() {
    auto size = yields_.size();
    std::vector<VariantType> values;
    values.reserve(size);

    for (auto *col : yields_) {
        auto expr = col->expr();
        auto v = expr->eval();
        if (!v.ok()) {
            return v.status();
        }
        values.emplace_back(v.value());
    }

    auto outputSchema = std::make_shared<SchemaWriter>();
    auto status = Collector::getSchema(
            values, resultColNames_, colTypes_, outputSchema.get());
    if (!status.ok()) {
        return status;
    }

    RowWriter writer(outputSchema);
    auto rsWriter = std::make_unique<RowSetWriter>(outputSchema);
    for (auto col : values) {
        status = Collector::collect(col, &writer);
        if (!status.ok()) {
            return status;
        }
    }
    rsWriter->addRow(writer);
    finishExecution(std::move(rsWriter));
    return Status::OK();
}

void YieldExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
    auto outputs = std::make_unique<InterimResult>(std::move(resultColNames_));
    if (rsWriter != nullptr) {
        outputs->setInterim(std::move(rsWriter));
    }

    if (onResult_) {
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        auto colNames = outputs->getColNames();
        resp_->set_column_names(std::move(colNames));
        if (outputs->hasData()) {
            auto ret = outputs->getRows();
            if (!ret.ok()) {
                LOG(ERROR) << "Get rows failed: " << ret.status();
                onError_(std::move(ret).status());
                return;
            }
            resp_->set_rows(std::move(ret).value());
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
