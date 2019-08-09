/* Copyright (c) 2019 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "base/Base.h"
#include "graph/GroupByExecutor.h"
#include "AggregateFunction.h"

namespace nebula {
namespace graph {

GroupByExecutor::GroupByExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<GroupBySentence*>(sentence);
}


Status GroupByExecutor::prepare() {
    expCtx_ = std::make_unique<ExpressionContext>();
    Status status;
    do {
        status = prepareYield();
        if (!status.ok()) {
            break;
        }

        status = prepareGroup();
        if (!status.ok()) {
            break;
        }
    } while (false);

    if (!status.ok()) {
        LOG(ERROR) << "Preparing failed: " << status;
        return status;
    }

    return status;
}


Status GroupByExecutor::prepareYield() {
    auto status = Status::OK();
    do {
        auto *clause = sentence_->yieldClause();
        std::vector<YieldColumn*> yields;
        if (clause != nullptr) {
            yields = clause->columns();
        }

        if (yields.empty()) {
            status = Status::Error("Yield cols is empty");
            break;
        }
        for (auto *col : yields) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            yieldCols_.emplace_back(std::make_pair(col, -1));

            if (col->alias() != nullptr) {
                if (col->expr()->isInputExpression()) {
                    auto inputName = static_cast<InputPropertyExpression*>(col->expr())->prop();
                    LOG(INFO) << "Add alias name " << *col->alias();
                    aliases_.emplace(*col->alias(), *inputName);
                }
            }
        }
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}


Status GroupByExecutor::prepareGroup() {
    auto status = Status::OK();
    do {
        auto *clause = sentence_->groupClause();
        std::vector<YieldColumn*> groups;
        if (clause != nullptr) {
            groups = clause->columns();
        }

        if (groups.empty()) {
            status = Status::Error("Group cols is empty");
            break;
        }
        for (auto *col : groups) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            groupCols_.emplace_back(std::make_pair(col, -1));
        }
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}


Status GroupByExecutor::buildIndex() {
    // Build index of input data for group cols
    std::unordered_map<std::string, std::pair<int64_t, nebula::cpp2::ValueType>> schemaMap;
    for (auto i = 0u; i < inputs_->schema()->getNumFields(); i++) {
        auto fieldName = inputs_->schema()->getFieldName(i);
        schemaMap[fieldName] = std::make_pair(i, inputs_->schema()->getFieldType(i));
    }

    for (auto &it : groupCols_) {
        // get input index
        if (it.first->expr()->isInputExpression()) {
            auto groupName = static_cast<InputPropertyExpression*>(it.first->expr())->prop();
            auto findIt = schemaMap.find(*groupName);
            if (findIt == schemaMap.end()) {
                LOG(ERROR) << "Group `" << *groupName << "' isn't in output fields";
                return Status::Error("Group `%s' isn't in output fields", *groupName);
            }
            it.second = findIt->second.first;
            continue;
        }

        if (it.first->getFunction() != F_NONE) {
            continue;
        }

        // get alias index
        auto groupName = it.first->expr()->toString();
        auto alisaIt = aliases_.find(groupName);
        if (alisaIt != aliases_.end()) {
            auto inputName = alisaIt->second;
            auto findIt = schemaMap.find(inputName);
            if (findIt == schemaMap.end()) {
                LOG(ERROR) << "Group `" << groupName << "' isn't in output fields";
                return Status::Error("Group `%s' isn't in output fields", groupName);
            }
            it.second = findIt->second.first;
            continue;
        }
        return Status::Error("Group `%s' isn't in output fields", groupName);
    }

    // Build index of input data for yield cols
    for (auto &it : yieldCols_) {
        if (it.first->expr()->isInputExpression()) {
            auto yieldName = static_cast<InputPropertyExpression*>(it.first->expr())->prop();
            auto findIt = schemaMap.find(*yieldName);
            if (findIt == schemaMap.end()) {
                LOG(ERROR) << "Yield `" << *yieldName << "' isn't in output fields";
                return Status::Error("Yield `%s' isn't in output fields", *yieldName);
            }
            it.second = findIt->second.first;
        }
        // TODO(laura): to check other expr
    }
    return Status::OK();
}


void GroupByExecutor::execute() {
    FLOG_INFO("Executing Group by: %s", sentence_->toString().c_str());

    if (rows_.empty()) {
        DCHECK(onFinish_);
        onFinish_();
        return;
    }

    auto status = buildIndex();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    GroupingData();

    generateOutputSchema();

    if (onResult_) {
        auto outputs = setupInterimResult();
        onResult_(std::move(outputs));
    }

    DCHECK(onFinish_);
    onFinish_();
}


cpp2::ColumnValue GroupByExecutor::toColumnValue(const VariantType& value) {
    cpp2::ColumnValue colVal;
    try {
        switch (value.which()) {
            case VAR_INT64:
                colVal.set_integer(boost::get<int64_t>(value));
                break;
            case VAR_DOUBLE:
                colVal.set_double_precision(boost::get<double>(value));
                break;
            case VAR_BOOL:
                colVal.set_bool_val(boost::get<bool>(value));
                break;
            case VAR_STR:
                colVal.set_str(boost::get<std::string>(value));
                break;
            default:
                LOG(ERROR) << "Wrong Type: " << value.which();
                colVal.set_str("");
                break;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception caught: " << e.what();
    }
    return colVal;
}


VariantType GroupByExecutor::toVariantType(const cpp2::ColumnValue& value) {
    switch (value.getType()) {
        case cpp2::ColumnValue::Type::integer:
            return value.get_integer();
        case cpp2::ColumnValue::Type::bool_val:
            return value.get_bool_val();
        case cpp2::ColumnValue::Type::double_precision:
            return value.get_double_precision();
        case cpp2::ColumnValue::Type::str:
            return value.get_str();
        default:
            break;
    }
    // TODO(laura): handle ERROR
    return "";
}


void GroupByExecutor::GroupingData() {
    // key : group col vals, val: cal funptr
    using FunCols = std::vector<std::shared_ptr<AggFun>>;
    using GroupData = std::unordered_map<ColVals, FunCols, ColsHasher>;
    std::unordered_map<FunKind, std::function<std::shared_ptr<AggFun>()>> funVec = {
            { F_NONE, []() -> auto {return std::make_shared<Group>();} },
            { F_COUNT, []() -> auto {return std::make_shared<Count>();} },
            { F_COUNT_DISTINCT, []() -> auto {return std::make_shared<CountDistinct>();} },
            { F_SUM, []() -> auto {return std::make_shared<Sum>();} },
            { F_AVG, []() -> auto {return std::make_shared<Avg>();} },
            { F_MAX, []() -> auto {return std::make_shared<Max>();} },
            { F_MIN, []() -> auto {return std::make_shared<Min>();} },
            { F_STD, []() -> auto {return std::make_shared<Stdev>();} },
            { F_BIT_AND, []() -> auto {return std::make_shared<BitAnd>();} },
            { F_BIT_OR, []() -> auto {return std::make_shared<BitOr>();} },
            { F_BIT_XOR, []() -> auto {return std::make_shared<BitXor>();} }
    };

    GroupData data;
    for (auto& it : rows_) {
        ColVals groupVals;
        FunCols calVals;

        // Firstly: group the cols
        for (auto &col : groupCols_) {
            auto index = col.second;
            auto &getters = expCtx_->getters();
            getters.getInputProp = [&] (const std::string &) -> VariantType {
                CHECK_GE(index, 0);
                auto val = it.columns[index];
                return toVariantType(val);
            };
            // TODO(Laura): `eval' may fail
            auto eval = col.first->expr()->eval();
            auto value = toColumnValue(eval);
            groupVals.vec.emplace_back(value);
        }

        auto findIt = data.find(groupVals);

        // Secondly: get the cal col

        // Init fun handler
        if (findIt == data.end()) {
            for (auto &col : yieldCols_) {
                auto funPtr = funVec[col.first->getFunction()]();
                calVals.emplace_back(funPtr);
            }
        } else {
            calVals = findIt->second;
        }

        // Apply value
        auto i = 0u;
        for (auto &col : calVals) {
            auto index = yieldCols_[i].second;
            auto &getters = expCtx_->getters();
            getters.getInputProp = [&] (const std::string &) -> VariantType {
                auto val = it.columns[index];
                return toVariantType(val);
            };
            auto *expr = yieldCols_[i].first->expr();
            // TODO(laura): `eval' may fail;
            auto value = toColumnValue(expr->eval());
            col->apply(value);
            i++;
        }

        if (findIt == data.end()) {
            data.emplace(groupVals, calVals);
        }
    }

    // Generate result data
    rows_.clear();
    for (auto& item : data) {
        std::vector<cpp2::ColumnValue> row;
        for (auto& col : item.second) {
            row.emplace_back(col->getResult());
        }
        rows_.emplace_back();
        rows_.back().set_columns(std::move(row));
    }
}


std::vector<std::string> GroupByExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yieldCols_.size());
    for (auto col : yieldCols_) {
        if (col.first->alias() == nullptr) {
            result.emplace_back(col.first->toString());
        } else {
            result.emplace_back(*col.first->alias());
        }
    }
    return result;
}


void GroupByExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    if (result == nullptr) {
        LOG(INFO) << "result is nullptr";
        return;
    }
    inputs_ = std::move(result);
    rows_ = inputs_->getRows();
}


void GroupByExecutor::generateOutputSchema() {
    using nebula::cpp2::SupportedType;
    if (resultSchema_ == nullptr) {
        resultSchema_ = std::make_shared<SchemaWriter>();
        auto colnames = getResultColumnNames();
        CHECK(!rows_.empty());
        for (auto i = 0u; i < rows_[0].columns.size(); i++) {
            SupportedType type;
            switch (rows_[0].columns[i].getType()) {
                case cpp2::ColumnValue::Type::bool_val:
                    type = SupportedType::BOOL;
                    break;
                case cpp2::ColumnValue::Type::integer:
                    type = SupportedType::INT;
                    break;
                case cpp2::ColumnValue::Type::str:
                    type = SupportedType::STRING;
                    break;
                case cpp2::ColumnValue::Type::double_precision:
                    type = SupportedType::DOUBLE;
                    break;
                default:
                    LOG(FATAL) << "Unknown VariantType: " << rows_[0].columns[i].getType();
            }
            // TODO(laura) : should handle exist colname
            resultSchema_->appendCol(colnames[i], type);
        }
    }
}


std::unique_ptr<InterimResult> GroupByExecutor::setupInterimResult() {
    if (rows_.empty() || resultSchema_ == nullptr) {
        return nullptr;
    }
    // Generic results
    std::unique_ptr<RowSetWriter> rsWriter = std::make_unique<RowSetWriter>(resultSchema_);

    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows_) {
        RowWriter writer(resultSchema_);
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
        if (rsWriter != nullptr) {
            rsWriter->addRow(writer);
        }
    }

    if (rsWriter == nullptr) {
        return nullptr;
    }
    return std::make_unique<InterimResult>(std::move(rsWriter));
}


void GroupByExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (rows_.empty() || resultSchema_ == nullptr) {
        return;
    }

    std::vector<std::string> columnNames;
    columnNames.reserve(resultSchema_->getNumFields());

    auto field = resultSchema_->begin();
    while (field) {
        columnNames.emplace_back(field->getName());
        ++field;
    }
    resp.set_column_names(std::move(columnNames));
    resp.set_rows(std::move(rows_));
}
}  // namespace graph
}  // namespace nebula
