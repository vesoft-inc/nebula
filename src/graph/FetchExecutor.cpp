/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "FetchExecutor.h"
#include "SchemaHelper.h"

namespace nebula {
namespace graph {

Status FetchExecutor::prepareYield() {
    if (yieldClause_ == nullptr) {
        setupColumns();
    } else {
        yields_ = yieldClause_->columns();
        // TODO 'distinct' could always pushdown in fetch.
        distinct_ = yieldClause_->isDistinct();
    }

    for (auto *col : yields_) {
        col->expr()->setContext(expCtx_.get());
        Status status = col->expr()->prepare();
        if (!status.ok()) {
            return status;
        }
        if (col->alias() == nullptr) {
            resultColNames_.emplace_back(col->expr()->toString());
            returnColNames_.emplace_back(col->expr()->toString());
        } else {
            resultColNames_.emplace_back(*col->alias());
            returnColNames_.emplace_back(*col->alias());
        }

        auto type = calculateExprType(col->expr());
        colTypes_.emplace_back(type);

        VLOG(1) << "type: " << static_cast<int64_t>(colTypes_.back());
    }

    if (expCtx_->hasSrcTagProp() || expCtx_->hasDstTagProp()) {
        return Status::SyntaxError(
                    "tag.prop and edgetype.prop are supported in fetch sentence.");
    }

    if (expCtx_->hasInputProp() || expCtx_->hasVariableProp()) {
        // TODO: support yield input and variable props
        return Status::SyntaxError(
                    "`$-' and `$variable' not supported in fetch yet.");
    }

    return Status::OK();
}

void FetchExecutor::setupColumns() {
    auto iter = DCHECK_NOTNULL(labelSchema_)->begin();
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
        resp_->set_column_names(std::move(returnColNames_));
    }
    resp = std::move(*resp_);
}

void FetchExecutor::onEmptyInputs() {
    if (onResult_) {
        auto outputs = std::make_unique<InterimResult>(std::move(returnColNames_));
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(returnColNames_));
    }
    doFinish(Executor::ProcessControl::kNext);
}

Status FetchExecutor::getOutputSchema(
        meta::SchemaProviderIf *schema,
        const RowReader *reader,
        SchemaWriter *outputSchema) const {
    if (expCtx_ == nullptr || resultColNames_.empty()) {
        return Status::Error("Input is empty.");
    }
    Getters getters;
    getters.getAliasProp = [schema, reader] (const std::string&, const std::string &prop) {
        return Collector::getProp(schema, prop, reader);
    };
    getters.getEdgeDstId = [schema,
                            reader] (const std::string&) -> OptVariantType {
        return Collector::getProp(schema, _DST, reader);
    };
    std::vector<VariantType> record;
    for (auto *column : yields_) {
        auto *expr = column->expr();
        auto value = expr->eval(getters);
        if (!value.ok()) {
            return value.status();
        }
        record.emplace_back(std::move(value.value()));
    }

    return Collector::getSchema(record, resultColNames_, colTypes_, outputSchema);
}

void FetchExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
    auto outputs = std::make_unique<InterimResult>(std::move(returnColNames_));
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
                doError(std::move(ret).status());
                return;
            }
            resp_->set_rows(std::move(ret).value());
        }
    }
    doFinish(Executor::ProcessControl::kNext);
}

void FetchExecutor::doEmptyResp() {
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    resp_->set_column_names(std::vector<std::string>());
    resp_->set_rows(std::vector<cpp2::RowValue>());
    doFinish(Executor::ProcessControl::kNext);
}

}  // namespace graph
}  // namespace nebula
