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

const char* kCount = "COUNT";
const char* kCountDist = "COUNT_DISTINCT";
const char* kSum = "SUM";
const char* kAvg = "AVG";
const char* kMax = "MAX";
const char* kMin = "MIN";
const char* kStd = "STD";
const char* kBitAnd = "BIT_AND";
const char* kBitOr = "BIT_OR";
const char* kBitXor = "BIT_XOR";

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
            if ((col->getFunName() != kCount && col->getFunName() != kCountDist)
                    && col->expr()->toString() == "*") {
                status = Status::Error("Syntax error: near `*'");
                break;
            }
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            yieldCols_.emplace_back(std::make_pair(col, -1));

            if (col->alias() != nullptr) {
                if (col->expr()->isInputExpression()) {
                    auto inputName = static_cast<InputPropertyExpression*>(col->expr())->prop();
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
            if (col->getFunName() != "") {
                status = Status::Error("Use invalid group function `%s'",
                                            col->getFunName().c_str());
                break;
            }
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
    for (auto i = 0u; i < schema_->getNumFields(); i++) {
        auto fieldName = schema_->getFieldName(i);
        schemaMap[fieldName] = std::make_pair(i, schema_->getFieldType(i));
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

        // function call
        if (it.first->expr()->isFunCallExpression()) {
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
        } else if (it.first->expr()->isVariableExpression()) {
            LOG(ERROR) << "Can't support variableExpression: " << it.first->expr()->toString();
            return Status::Error("Can't support variableExpression");
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

    status = groupingData();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

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


Status GroupByExecutor::groupingData() {
    // key : group col vals, val: cal funptr
    using FunCols = std::vector<std::shared_ptr<AggFun>>;
    using GroupData = std::unordered_map<ColVals, FunCols, ColsHasher>;
    static std::unordered_map<std::string, std::function<std::shared_ptr<AggFun>()>> funVec = {
            { "", []() -> auto { return std::make_shared<Group>();} },
            { kCount, []() -> auto { return std::make_shared<Count>();} },
            { kCountDist, []() -> auto { return std::make_shared<CountDistinct>();} },
            { kSum, []() -> auto { return std::make_shared<Sum>();} },
            { kAvg, []() -> auto { return std::make_shared<Avg>();} },
            { kMax, []() -> auto { return std::make_shared<Max>();} },
            { kMin, []() -> auto { return std::make_shared<Min>();} },
            { kStd, []() -> auto { return std::make_shared<Stdev>();} },
            { kBitAnd, []() -> auto { return std::make_shared<BitAnd>();} },
            { kBitOr, []() -> auto { return std::make_shared<BitOr>();} },
            { kBitXor, []() -> auto { return std::make_shared<BitXor>();} }
    };

    GroupData data;
    for (auto& it : rows_) {
        ColVals groupVals;
        FunCols calVals;

        // Firstly: group the cols
        for (auto &col : groupCols_) {
            auto index = col.second;
            auto &getters = expCtx_->getters();
            cpp2::ColumnValue val;
            getters.getInputProp = [&] (const std::string &) -> OptVariantType {
                CHECK_GE(index, 0);
                val = it.columns[index];
                return true;
            };

            auto eval = col.first->expr()->eval();
            if (!eval.ok()) {
                return eval.status();
            }
            if (val.getType() == cpp2::ColumnValue::Type::__EMPTY__) {
                val = toColumnValue(eval.value());
            }
            groupVals.vec.emplace_back(val);
        }

        auto findIt = data.find(groupVals);

        // Secondly: get the cal col

        // Init fun handler
        if (findIt == data.end()) {
            for (auto &col : yieldCols_) {
                auto funPtr = funVec[col.first->getFunName()]();
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

            cpp2::ColumnValue val;
            getters.getInputProp = [&] (const std::string &) -> OptVariantType{
                CHECK_GE(index, 0);
                val = it.columns[index];
                return true;
            };
            auto eval = yieldCols_[i].first->expr()->eval();
            if (!eval.ok()) {
                return eval.status();
            }
            if (val.getType() == cpp2::ColumnValue::Type::__EMPTY__) {
                val = toColumnValue(eval.value());
            }
            col->apply(val);
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

    return Status::OK();
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

    auto ret = result->getRows();
    if (!ret.ok()) {
        return;
    }
    rows_ = std::move(ret).value();
    schema_ = std::move(result->schema());
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
                case Type::id:
                    writer << column.get_id();
                    break;
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
                case Type::timestamp:
                    writer << column.get_timestamp();
                    break;
                default:
                    LOG(FATAL) << "Not Support: " << column.getType();
            }
        }
        if (rsWriter != nullptr) {
            rsWriter->addRow(writer);
        }
    }

    auto result = std::make_unique<InterimResult>(getResultColumnNames());
    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return result;
}


void GroupByExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (rows_.empty() || resultSchema_ == nullptr) {
        return;
    }

    resp.set_column_names(getResultColumnNames());
    resp.set_rows(std::move(rows_));
}
}  // namespace graph
}  // namespace nebula
