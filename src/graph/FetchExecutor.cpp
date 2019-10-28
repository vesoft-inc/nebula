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
        } else {
            resultColNames_.emplace_back(*col->alias());
        }

        // such as YIELD 1+1, it has not type in schema, the type from the eval()
        colTypes_.emplace_back(nebula::cpp2::SupportedType::UNKNOWN);
        if (col->expr()->isAliasExpression()) {
            colNames_.emplace_back(*dynamic_cast<AliasPropertyExpression*>(col->expr())->prop());
            continue;
        } else if (col->expr()->isTypeCastingExpression()) {
            // type cast
            auto exprPtr = dynamic_cast<TypeCastingExpression*>(col->expr());
            colTypes_.back() = ColumnTypeToSupportedType(exprPtr->getType());
        }

        colNames_.emplace_back(col->expr()->toString());
    }

    if (expCtx_->hasSrcTagProp() || expCtx_->hasDstTagProp()) {
        return Status::SyntaxError(
                    "tag.prop and edgetype.prop are supported in fetch sentence.");
    }

    auto aliasProps = expCtx_->aliasProps();
    for (auto pair : aliasProps) {
        if (pair.first != *labelName_) {
            return Status::SyntaxError(
                "Near [%s.%s], tag or edge should be declared in statement first.",
                    pair.first.c_str(), pair.second.c_str());
        }
    }

    return Status::OK();
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
        auto outputs = std::make_unique<InterimResult>(std::move(resultColNames_));
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}

Status FetchExecutor::getOutputSchema(
        meta::SchemaProviderIf *schema,
        const RowReader *reader,
        SchemaWriter *outputSchema) const {
    if (expCtx_ == nullptr || resultColNames_.empty()) {
        return Status::Error("Input is empty.");
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
        if (!value.ok()) {
            return value.status();
        }
        record.emplace_back(std::move(value.value()));
    }

    if (colTypes_.size() != record.size()) {
        return Status::Error("Input size is not equal to output");
    }
    using nebula::cpp2::SupportedType;
    auto index = 0u;
    for (auto &it : colTypes_) {
        SupportedType type;
        if (it == SupportedType::UNKNOWN) {
            switch (record[index].which()) {
                case VAR_INT64:
                    // all integers in InterimResult are regarded as type of INT
                    type = SupportedType::INT;
                    break;
                case VAR_DOUBLE:
                    type = SupportedType::DOUBLE;
                    break;
                case VAR_BOOL:
                    type = SupportedType::BOOL;
                    break;
                case VAR_STR:
                    type = SupportedType::STRING;
                    break;
                default:
                    std::string msg = folly::stringPrintf(
                            "Unknown VariantType: %d", record[index].which());
                    LOG(ERROR) << msg;
                    return Status::Error(msg);
            }
        } else {
            type = it;
        }

        outputSchema->appendCol(resultColNames_[index], type);
        index++;
    }
    return Status::OK();
}

void FetchExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
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
}  // namespace graph
}  // namespace nebula
