/* Copyright (c) 2019 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "base/Base.h"
#include "graph/GroupByExecutor.h"

namespace nebula {
namespace graph {

GroupByExecutor::GroupByExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<GroupBySentence*>(sentence);
}


Status GroupByExecutor::prepare() {
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

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
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        yields_ = clause->columns();
    }

    if (yields_.empty()) {
        return Status::Error("Yield cols is empty");
    }

    return Status::OK();
}


Status GroupByExecutor::prepareGroup() {
    auto *clause = sentence_->groupClause();
    std::vector<GroupColumn*>  groups;
    if (clause != nullptr) {
        groups = clause->columns();
    }

    if (groups.empty()) {
        return Status::Error("Group columns is empty");
    }

    // Conversion alias name to fieldName
    for (auto& it : groups) {
        auto find = false;
        auto groupCol = it->expr()->toString();
        groupCol.erase(std::remove(groupCol.begin(), groupCol.end(), '"'), groupCol.end());
        std::string fieldCol;
        for (auto *col : yields_) {
            fieldCol = col->expr()->toString();
            if (col->alias() == nullptr) {
                if (groupCol == col->expr()->toString()) {
                    find = true;
                    break;
                }
            } else {
                if (groupCol == fieldCol) {
                    find = true;
                    break;
                }
                if (groupCol == *col->alias()) {
                    find = true;
                    break;
                }
            }
        }
        if (find) {
            groupCols_.emplace(fieldCol, -1);
        } else {
            groupCols_.emplace(groupCol, -1);
        }
    }
    return Status::OK();
}


Status GroupByExecutor::buildIndex() {
    // Build index of input data for group cols
    for (auto &it : groupCols_) {
        auto find = false;
        auto groupName = it.first;
        for (auto i = 0u; i < schema_->getNumFields(); i++) {
            auto fieldName = schema_->getFieldName(i);
            if (groupName.size() > 3 && groupName.substr(3) == fieldName) {
                it.second = i;
                find = true;
                break;
            }
        }
        if (!find) {
            LOG(ERROR) << "Group `" << groupName << "' isn't in output fields";
            return Status::Error("Group `%s' isn't in output fields", groupName);
        }
    }

    // Build index of input data for yield cols
    for (auto &it : yields_) {
        auto find = false;
        auto yieldName = it->expr()->toString();
        for (auto i = 0u; i < schema_->getNumFields(); i++) {
            auto fieldName = schema_->getFieldName(i);
            // col name is *
            if (yieldName == "*" && it->getFunction() == F_COUNT) {
                ColType yCol;
                yCol.fieldName_ = yieldName;
                yCol.fun_ = it->getFunction();
                yCol.index_ = -i;
                yieldCols_.emplace_back(std::move(yCol));
                find = true;
                break;
            }
            // yield col has not calfun, but not in group cols
            if (it->getFunction() == F_NONE) {
                if (groupCols_.find(yieldName) == groupCols_.end()) {
                    LOG(ERROR) << "Yield `" << yieldName << "' isn't in group fields";
                    return Status::Error("Yield `%s' isn't in group fields", yieldName);
                }
            }
            if (yieldName.size() > 3 && yieldName.substr(3) == fieldName) {
                auto type = schema_->getFieldType(i);
                ColType yCol;
                yCol.fieldName_ = fieldName;
                yCol.fun_ = it->getFunction();
                yCol.index_ = i;
                // When COUNT type should be INT, or type of SUM and AVG is nonsupport
                if (yCol.fun_ == F_COUNT_DISTINCT || yCol.fun_ == F_COUNT ||
                    (yCol.fun_ == F_SUM && (yCol.type_ != nebula::cpp2::SupportedType::INT
                        || yCol.type_ != nebula::cpp2::SupportedType::DOUBLE)) ||
                    (yCol.fun_ == F_AVG && (yCol.type_ != nebula::cpp2::SupportedType::INT
                        || yCol.type_ != nebula::cpp2::SupportedType::DOUBLE))) {
                    yCol.type_ = nebula::cpp2::SupportedType::INT;
                } else {
                    yCol.type_ = type.type;
                }

                yieldCols_.emplace_back(std::move(yCol));
                find = true;
                break;
            }
        }
        if (!find) {
            LOG(ERROR) << "Yield `" << yieldName << "' isn't in output fields";
            return Status::Error("Yield `%s' isn't in output fields", yieldName);
        }
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


void GroupByExecutor::GroupingData() {
    // key : group col vals, val: cal ptr
    using FunCols = std::vector<std::shared_ptr<AggFun>>;
    using GroupData = std::unordered_map<ColVals, FunCols, ColsHasher>;
    GroupData data;

    for (auto& it : rows_) {
        ColVals groupCols;
        FunCols calCols;
        // firstly:  group the cols
        for (auto &col : groupCols_) {
            groupCols.vec.emplace_back(it.columns[col.second]);
        }
        auto findIt = data.find(groupCols);

        // secondly: get the cal cols
        auto i = 0u;
        for (auto &col : yieldCols_) {
            switch (col.fun_) {
                case F_NONE: {
                    std::shared_ptr<AggFun> basePtr;
                    if (findIt == data.end()) {
                        auto groupPtr = std::make_shared<Group>();
                        groupPtr->apply(it.columns[col.index_]);
                        calCols.emplace_back(std::move(groupPtr));
                    } else {
                        findIt->second[i]->apply(it.columns[col.index_]);
                    }
                    break;
                }
                case F_COUNT: {
                    std::shared_ptr<AggFun> basePtr;
                    if (findIt == data.end()) {
                        auto countPtr = std::make_shared<Count>();
                        countPtr->apply();
                        calCols.emplace_back(std::move(countPtr));
                    } else {
                        findIt->second[i]->apply();
                    }
                    break;
                }
                case F_COUNT_DISTINCT: {
                    std::shared_ptr<AggFun> basePtr;
                    if (findIt == data.end()) {
                        auto countDisPtr = std::make_shared<CountDistinct>();
                        countDisPtr->apply(it.columns[col.index_]);
                        calCols.emplace_back(std::move(countDisPtr));
                    } else {
                        basePtr = findIt->second[i];
                        findIt->second[i]->apply(it.columns[col.index_]);
                    }
                    break;
                }
                case F_SUM: {
                    std::shared_ptr<AggFun> basePtr;
                    if (findIt == data.end()) {
                        auto sumPtr = std::make_shared<Sum>();
                        sumPtr->apply(it.columns[col.index_]);
                        calCols.emplace_back(std::move(sumPtr));
                    } else {
                        findIt->second[i]->apply(it.columns[col.index_]);
                    }
                    break;
                }
                case F_AVG: {
                    std::shared_ptr<AggFun> basePtr;
                    if (findIt == data.end()) {
                        auto avgPtr = std::make_shared<Avg>();
                        avgPtr->apply(it.columns[col.index_]);
                        calCols.emplace_back(std::move(avgPtr));
                    } else {
                        findIt->second[i]->apply(it.columns[col.index_]);
                    }
                    break;
                }
                case F_MAX : {
                    std::shared_ptr<AggFun> basePtr;
                    if (findIt == data.end()) {
                        auto maxPtr = std::make_shared<Max>();
                        maxPtr->apply(it.columns[col.index_]);
                        calCols.emplace_back(std::move(maxPtr));
                    } else {
                        findIt->second[i]->apply(it.columns[col.index_]);
                    }
                    break;
                }
                case F_MIN : {
                    if (findIt == data.end()) {
                        auto minPtr = std::make_shared<Min>();
                        minPtr->apply(it.columns[col.index_]);
                        calCols.emplace_back(std::move(minPtr));
                    } else {
                        findIt->second[i]->apply(it.columns[col.index_]);
                    }
                    break;
                }
                default:
                    break;
            }
            i++;
        }
        if (findIt == data.end()) {
            data.emplace(groupCols, calCols);
        }
    }

    // generate result data
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
    result.reserve(yields_.size());
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}


void GroupByExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    if (result == nullptr) {
        LOG(INFO) << "result is nullptr";
        return;
    }
    rows_ = result->getRows();
    schema_ = std::move(result->schema());
}


void GroupByExecutor::generateOutputSchema() {
    if (resultSchema_ == nullptr) {
        resultSchema_ = std::make_shared<SchemaWriter>();
        auto colnames = getResultColumnNames();
        CHECK(colnames.size() == yieldCols_.size());
        for (auto i = 0u; i < colnames.size(); i++) {
            resultSchema_->appendCol(colnames[i], yieldCols_[i].type_);
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
        rsWriter->addRow(writer);
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
}   // namespace graph
}   // namespace nebula
