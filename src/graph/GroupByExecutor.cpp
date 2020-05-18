/* Copyright (c) 2019 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "base/Base.h"
#include "graph/GroupByExecutor.h"
#include "graph/AggregateFunction.h"

namespace nebula {
namespace graph {

GroupByExecutor::GroupByExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "group_by") {
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
    auto *clause = sentence_->yieldClause();
    std::vector<YieldColumn*> yields;
    if (clause != nullptr) {
        yields = clause->columns();
    }

    if (yields.empty()) {
        return Status::SyntaxError("Yield cols is empty");
    }
    for (auto *col : yields) {
        std::string aggFun;
        if ((col->getFunName() != kCount && col->getFunName() != kCountDist)
                && col->expr()->toString() == "*") {
            return Status::SyntaxError("Syntax error: near `*'");
        }

        col->expr()->setContext(expCtx_.get());
        status = col->expr()->prepare();
        if (!status.ok()) {
            LOG(ERROR) << status;
            return status;
        }
        yieldCols_.emplace_back(col);

        if (col->alias() != nullptr) {
            if (col->expr()->isInputExpression()) {
                aliases_.emplace(*col->alias(), col);
            }
        }
    }

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
            status = Status::SyntaxError("Group cols is empty");
            break;
        }
        for (auto *col : groups) {
            if (col->getFunName() != "") {
                status = Status::SyntaxError("Use invalid group function `%s'",
                                              col->getFunName().c_str());
                break;
            }
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            groupCols_.emplace_back(col);
        }
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}


Status GroupByExecutor::checkAll() {
    // Get index of input data
    for (auto i = 0u; i < schema_->getNumFields(); i++) {
        auto fieldName = schema_->getFieldName(i);
        schemaMap_[fieldName] = i;
    }

    // Check group col
    std::unordered_set<std::string> inputGroupCols;
    for (auto &it : groupCols_) {
        // Check input col
        if (it->expr()->isInputExpression()) {
            auto groupName = static_cast<InputPropertyExpression*>(it->expr())->prop();
            auto findIt = schemaMap_.find(*groupName);
            if (findIt == schemaMap_.end()) {
                LOG(ERROR) << "Group `" << *groupName << "' isn't in output fields";
                return Status::SyntaxError("Group `%s' isn't in output fields", groupName->c_str());
            }
            inputGroupCols.emplace(*groupName);
            continue;
        }

        // Function call
        if (it->expr()->isFunCallExpression()) {
            continue;
        }

        // Check alias col
        auto groupName = it->expr()->toString();
        auto alisaIt = aliases_.find(groupName);
        if (alisaIt != aliases_.end()) {
            it = alisaIt->second;
            auto gName = static_cast<InputPropertyExpression*>(it->expr())->prop();
            if (it->expr()->isInputExpression()) {
                inputGroupCols.emplace(*gName);
            }
            continue;
        }
        return Status::SyntaxError("Group `%s' isn't in output fields", groupName.c_str());
    }

    // Check yield cols
    for (auto &it : yieldCols_) {
        if (it->expr()->isInputExpression()) {
            auto yieldName = static_cast<InputPropertyExpression*>(it->expr())->prop();
            auto findIt = schemaMap_.find(*yieldName);
            if (findIt == schemaMap_.end()) {
                LOG(ERROR) << "Yield `" << *yieldName << "' isn't in output fields";
                return Status::SyntaxError("Yield `%s' isn't in output fields", yieldName->c_str());
            }
            // Check input yield filed without agg fun and not in group cols
            if (inputGroupCols.find(*yieldName) == inputGroupCols.end() &&
                    it->getFunName().empty()) {
                LOG(ERROR) << "Yield `" << *yieldName << "' isn't in group fields";
                return Status::SyntaxError("Yield `%s' isn't in group fields", yieldName->c_str());
            }
        } else if (it->expr()->isVariableExpression()) {
            LOG(ERROR) << "Can't support variableExpression: " << it->expr()->toString();
            return Status::SyntaxError("Can't support variableExpression");
        }
    }
    return Status::OK();
}


void GroupByExecutor::execute() {
    if (inputs_ == nullptr || !inputs_->hasData()) {
        onEmptyInputs();
        return;
    }

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto ret = inputs_->getRows();
    if (!ret.ok()) {
        LOG(ERROR) << "Get rows failed: " << ret.status();
        doError(std::move(ret).status());
        return;
    }
    rows_ = std::move(ret).value();
    if (rows_.empty()) {
        onEmptyInputs();
        return;
    }
    schema_ = inputs_->schema();

    status = checkAll();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    status = groupingData();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    status = generateOutputSchema();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    if (onResult_) {
        auto result = setupInterimResult();
        if (!result.ok()) {
            doError(std::move(result).status());
            return;
        }
        onResult_(std::move(result).value());
    }

    doFinish(Executor::ProcessControl::kNext);
}


Status GroupByExecutor::groupingData() {
    using FunCols = std::vector<std::shared_ptr<AggFun>>;
    // key : the column values of group by, val: function table of aggregated columns
    using GroupData = std::unordered_map<ColVals, FunCols, ColsHasher>;

    GroupData data;
    Getters getters;
    for (auto& it : rows_) {
        ColVals groupVals;
        FunCols calVals;

        // Firstly: group the cols
        for (auto &col : groupCols_) {
            cpp2::ColumnValue::Type valType = cpp2::ColumnValue::Type::__EMPTY__;
            getters.getInputProp = [&] (const std::string & prop) -> OptVariantType {
                auto indexIt = schemaMap_.find(prop);
                if (indexIt == schemaMap_.end()) {
                    LOG(ERROR) << prop <<  " is nonexistent";
                    return Status::Error("%s is nonexistent", prop.c_str());
                }
                auto val = it.columns[indexIt->second];
                valType = val.getType();
                return toVariantType(val);
            };

            auto eval = col->expr()->eval(getters);
            if (!eval.ok()) {
                return eval.status();
            }

            auto cVal = toColumnValue(eval.value(), valType);
            if (!cVal.ok()) {
                return cVal.status();
            }
            groupVals.vec.emplace_back(std::move(cVal).value());
        }

        auto findIt = data.find(groupVals);

        // Secondly: get the value of the aggregated column

        // Get all aggregation function
        if (findIt == data.end()) {
            for (auto &col : yieldCols_) {
                auto funPtr = funVec[col->getFunName()]();
                calVals.emplace_back(std::move(funPtr));
            }
        } else {
            calVals = findIt->second;
        }

        // Apply value
        auto i = 0u;
        for (auto &col : calVals) {
            cpp2::ColumnValue::Type valType = cpp2::ColumnValue::Type::__EMPTY__;
            getters.getInputProp = [&] (const std::string &prop) -> OptVariantType{
                auto indexIt = schemaMap_.find(prop);
                if (indexIt == schemaMap_.end()) {
                    LOG(ERROR) << prop <<  " is nonexistent";
                    return Status::Error("%s is nonexistent", prop.c_str());
                }
                auto val = it.columns[indexIt->second];
                valType = val.getType();
                return toVariantType(val);
            };
            auto eval = yieldCols_[i]->expr()->eval(getters);
            if (!eval.ok()) {
                return eval.status();
            }

            auto cVal = toColumnValue(std::move(eval).value(), valType);
            if (!cVal.ok()) {
                return cVal.status();
            }
            col->apply(cVal.value());
            i++;
        }

        if (findIt == data.end()) {
            data.emplace(std::move(groupVals), std::move(calVals));
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

    return Status::OK();
}


std::vector<std::string> GroupByExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yieldCols_.size());
    for (auto col : yieldCols_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}


Status GroupByExecutor::generateOutputSchema() {
    CHECK(!rows_.empty());
    auto& row = rows_[0];
    using nebula::cpp2::SupportedType;
    if (resultSchema_ == nullptr) {
        resultSchema_ = std::make_shared<SchemaWriter>();
        auto colnames = getResultColumnNames();
        for (auto i = 0u; i < row.columns.size(); i++) {
            SupportedType type;
            switch (row.columns[i].getType()) {
                case cpp2::ColumnValue::Type::id:
                    type = SupportedType::VID;
                    break;
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
                case cpp2::ColumnValue::Type::timestamp:
                    type = SupportedType::TIMESTAMP;
                    break;
                default:
                    LOG(ERROR) << "Unknown VariantType: " << row.columns[i].getType();
                    return Status::Error("Unknown VariantType: %d", row.columns[i].getType());
            }
            // TODO(laura) : should handle exist colname
            resultSchema_->appendCol(colnames[i], type);
        }
    }
    return Status::OK();
}


StatusOr<std::unique_ptr<InterimResult>> GroupByExecutor::setupInterimResult() {
    auto result = std::make_unique<InterimResult>(getResultColumnNames());
    if (rows_.empty() || resultSchema_ == nullptr) {
        return result;
    }
    // Generate results
    std::unique_ptr<RowSetWriter> rsWriter = std::make_unique<RowSetWriter>(resultSchema_);

    InterimResult::getResultWriter(rows_, rsWriter.get());
    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return result;
}


void GroupByExecutor::onEmptyInputs() {
    if (onResult_) {
        auto result = std::make_unique<InterimResult>(getResultColumnNames());
        onResult_(std::move(result));
    }
    doFinish(Executor::ProcessControl::kNext);
}


void GroupByExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp.set_column_names(getResultColumnNames());

    if (rows_.empty()) {
        return;
    }
    resp.set_rows(std::move(rows_));
}
}  // namespace graph
}  // namespace nebula
