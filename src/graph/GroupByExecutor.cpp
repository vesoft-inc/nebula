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
            status = Status::SyntaxError("Yield cols is empty");
            break;
        }
        for (auto *col : yields) {
            if ((col->getFunName() != kCount && col->getFunName() != kCountDist)
                    && col->expr()->toString() == "*") {
                status = Status::SyntaxError("Syntax error: near `*'");
                break;
            }
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            yieldCols_.emplace_back(col);

            if (col->alias() != nullptr) {
                if (col->expr()->isInputExpression()) {
                    aliases_.emplace(*col->alias(), col);
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

    auto status = checkAll();
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


cpp2::ColumnValue GroupByExecutor::toColumnValue(const VariantType& value,
                                                 cpp2::ColumnValue::Type type) {
    cpp2::ColumnValue colVal;
    try {
        if (type == cpp2::ColumnValue::Type::__EMPTY__) {
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
            return colVal;
        }
        switch (type) {
            case cpp2::ColumnValue::Type::id:
                colVal.set_id(boost::get<int64_t>(value));
                break;
            case cpp2::ColumnValue::Type::integer:
                colVal.set_integer(boost::get<int64_t>(value));
                break;
            case cpp2::ColumnValue::Type::timestamp:
                colVal.set_timestamp(boost::get<int64_t>(value));
                break;
            case cpp2::ColumnValue::Type::double_precision:
                colVal.set_double_precision(boost::get<double>(value));
                break;
            case cpp2::ColumnValue::Type::bool_val:
                colVal.set_bool_val(boost::get<bool>(value));
                break;
            case cpp2::ColumnValue::Type::str:
                colVal.set_str(boost::get<std::string>(value));
                break;
            default:
                LOG(ERROR) << "Wrong Type: " << static_cast<int32_t>(type);
                colVal.set_str("");
                break;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        colVal.set_str("");
    }
    return colVal;
}


VariantType GroupByExecutor::toVariantType(const cpp2::ColumnValue& value) {
    switch (value.getType()) {
        case cpp2::ColumnValue::Type::id:
            return value.get_id();
        case cpp2::ColumnValue::Type::integer:
            return value.get_integer();
        case cpp2::ColumnValue::Type::bool_val:
            return value.get_bool_val();
        case cpp2::ColumnValue::Type::double_precision:
            return value.get_double_precision();
        case cpp2::ColumnValue::Type::str:
            return value.get_str();
        case cpp2::ColumnValue::Type::timestamp:
            return value.get_timestamp();
        default:
            LOG(ERROR) << "Unknown ColumnType: " << static_cast<int32_t>(value.getType());
            break;
    }
    return "";
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
            auto &getters = expCtx_->getters();
            cpp2::ColumnValue::Type valType = cpp2::ColumnValue::Type::__EMPTY__;
            getters.getInputProp = [&] (const std::string & prop) -> OptVariantType {
                auto indexIt = schemaMap_.find(prop);
                if (indexIt == schemaMap_.end()) {
                    LOG(ERROR) << prop <<  " is nonexistent";
                    return Status::Error("%s is nonexistent", prop.c_str());
                }
                CHECK_GE(indexIt->second, 0);
                auto val = it.columns[indexIt->second];
                valType = val.getType();
                return toVariantType(val);
            };

            auto eval = col->expr()->eval();
            if (!eval.ok()) {
                return eval.status();
            }

            auto val = toColumnValue(eval.value(), valType);
            groupVals.vec.emplace_back(std::move(val));
        }

        auto findIt = data.find(groupVals);

        // Secondly: get the cal col

        // Init fun handler
        if (findIt == data.end()) {
            for (auto &col : yieldCols_) {
                auto funPtr = funVec[col->getFunName()]();
                calVals.emplace_back(funPtr);
            }
        } else {
            calVals = findIt->second;
        }

        // Apply value
        auto i = 0u;
        for (auto &col : calVals) {
            auto &getters = expCtx_->getters();
            cpp2::ColumnValue::Type valType = cpp2::ColumnValue::Type::__EMPTY__;
            getters.getInputProp = [&] (const std::string &prop) -> OptVariantType{
                auto indexIt = schemaMap_.find(prop);
                if (indexIt == schemaMap_.end()) {
                    LOG(ERROR) << prop <<  " is nonexistent";
                    return Status::Error("%s is nonexistent");
                }
                CHECK_GE(indexIt->second, 0);
                auto val = it.columns[indexIt->second];
                valType = val.getType();
                return toVariantType(val);
            };
            auto eval = yieldCols_[i]->expr()->eval();
            if (!eval.ok()) {
                return eval.status();
            }

            auto val = toColumnValue(eval.value(), valType);
            col->apply(val);
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
