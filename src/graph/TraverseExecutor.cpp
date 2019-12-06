/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "parser/TraverseSentences.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "graph/GoExecutor.h"
#include "graph/PipeExecutor.h"
#include "graph/OrderByExecutor.h"
#include "graph/FetchVerticesExecutor.h"
#include "graph/FetchEdgesExecutor.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "graph/SetExecutor.h"
#include "graph/FindExecutor.h"
#include "graph/MatchExecutor.h"
#include "graph/FindPathExecutor.h"
#include "graph/LimitExecutor.h"
#include "graph/YieldExecutor.h"
#include "graph/GroupByExecutor.h"

namespace nebula {
namespace graph {

std::unique_ptr<TraverseExecutor> TraverseExecutor::makeTraverseExecutor(Sentence *sentence) {
    return makeTraverseExecutor(sentence, ectx());
}


// static
std::unique_ptr<TraverseExecutor>
TraverseExecutor::makeTraverseExecutor(Sentence *sentence, ExecutionContext *ectx) {
    auto kind = sentence->kind();
    std::unique_ptr<TraverseExecutor> executor;
    switch (kind) {
        case Sentence::Kind::kGo:
            executor = std::make_unique<GoExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kPipe:
            executor = std::make_unique<PipeExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kOrderBy:
            executor = std::make_unique<OrderByExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFetchVertices:
            executor = std::make_unique<FetchVerticesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFetchEdges:
            executor = std::make_unique<FetchEdgesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kSet:
            executor = std::make_unique<SetExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kMatch:
            executor = std::make_unique<MatchExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFind:
            executor = std::make_unique<FindExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kYield:
            executor = std::make_unique<YieldExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFindPath:
            executor = std::make_unique<FindPathExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kLimit:
            executor = std::make_unique<LimitExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::KGroupBy:
            executor = std::make_unique<GroupByExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kUnknown:
            LOG(FATAL) << "Sentence kind unknown";
            break;
        default:
            LOG(FATAL) << "Sentence kind illegal: " << kind;
            break;
    }
    return executor;
}

Status Collector::collect(VariantType &var, RowWriter *writer) {
    switch (var.which()) {
        case VAR_INT64:
            (*writer) << boost::get<int64_t>(var);
            break;
        case VAR_DOUBLE:
            (*writer) << boost::get<double>(var);
            break;
        case VAR_BOOL:
            (*writer) << boost::get<bool>(var);
            break;
        case VAR_STR:
            (*writer) << boost::get<std::string>(var);
            break;
        default:
            std::string errMsg =
                folly::stringPrintf("Unknown VariantType: %d", var.which());
            LOG(ERROR) << errMsg;
            return Status::Error(errMsg);
    }
    return Status::OK();
}

OptVariantType Collector::getProp(const meta::SchemaProviderIf *schema,
                               const std::string &prop,
                               const RowReader *reader) {
    DCHECK(reader != nullptr);
    DCHECK(schema != nullptr);
    using nebula::cpp2::SupportedType;
    auto type = schema->getFieldType(prop).type;
    switch (type) {
        case SupportedType::BOOL: {
            bool v;
            reader->getBool(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::TIMESTAMP:
        case SupportedType::INT: {
            int64_t v;
            reader->getInt(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::VID: {
            VertexID v;
            reader->getVid(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::FLOAT: {
            float v;
            reader->getFloat(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return static_cast<double>(v);
        }
        case SupportedType::DOUBLE: {
            double v;
            reader->getDouble(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::STRING: {
            folly::StringPiece v;
            reader->getString(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v.toString();
        }
        default:
            std::string errMsg =
                folly::stringPrintf("Unknown type: %d", static_cast<int32_t>(type));
            LOG(ERROR) << errMsg;
            return Status::Error(errMsg);
    }
}

Status Collector::getSchema(const std::vector<VariantType> &vals,
                          const std::vector<std::string> &colNames,
                          const std::vector<nebula::cpp2::SupportedType> &colTypes,
                          SchemaWriter *outputSchema) {
    DCHECK(outputSchema != nullptr);
    DCHECK_EQ(vals.size(), colNames.size());
    DCHECK_EQ(vals.size(), colTypes.size());
    using nebula::cpp2::SupportedType;
    auto index = 0u;
    for (auto &it : colTypes) {
        SupportedType type;
        if (it == SupportedType::UNKNOWN) {
            switch (vals[index].which()) {
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
                    std::string errMsg =
                        folly::stringPrintf("Unknown VariantType: %d", vals[index].which());
                    LOG(ERROR) << errMsg;
                    return Status::Error(errMsg);
            }
        } else {
            type = it;
        }

        outputSchema->appendCol(colNames[index], type);
        index++;
    }
    return Status::OK();
}

Status YieldClauseWrapper::prepare(
        const InterimResult *inputs,
        const VariableHolder *varHolder,
        std::vector<YieldColumn*> &yields) {
    auto cols = clause_->columns();
    yieldColsHolder_ = std::make_unique<YieldColumns>();
    for (auto *col : cols) {
        if (col->expr()->isInputExpression()) {
            if (inputs == nullptr) {
                return Status::Error("Inputs nullptr.");
            }
            if (needAllPropsFromInput(col, inputs, yields)) {
                continue;
            }
        } else if (col->expr()->isVariableExpression()) {
            if (varHolder == nullptr) {
                return Status::Error("VarHolder nullptr.");
            }
            auto ret = needAllPropsFromVar(col, varHolder, yields);
            if (!ret.ok()) {
                return std::move(ret).status();
            }
            if (ret.value()) {
                continue;
            }
        }
        yields.emplace_back(col);
    }

    return Status::OK();
}

bool YieldClauseWrapper::needAllPropsFromInput(const YieldColumn *col,
                                               const InterimResult *inputs,
                                               std::vector<YieldColumn*> &yields) {
    auto *inputExpr = static_cast<InputPropertyExpression*>(col->expr());
    auto *colName = inputExpr->prop();
    if (*colName == "*") {
        auto colNames = inputs->getColNames();
        for (auto &prop : colNames) {
            Expression *expr = new InputPropertyExpression(new std::string(prop));
            YieldColumn *column = new YieldColumn(expr);
            yieldColsHolder_->addColumn(column);
            yields.emplace_back(column);
        }
        return true;
    }
    return false;
}

StatusOr<bool> YieldClauseWrapper::needAllPropsFromVar(
                                             const YieldColumn *col,
                                             const VariableHolder *varHolder,
                                             std::vector<YieldColumn*> &yields) {
    auto *variableExpr = static_cast<VariablePropertyExpression*>(col->expr());
    auto *colName = variableExpr->prop();
    bool existing = false;
    auto *varname = variableExpr->alias();
    auto varInputs = varHolder->get(*varname, &existing);
    if (varInputs == nullptr && !existing) {
        return Status::Error("Variable `%s' not defined.", varname->c_str());
    }
    if (*colName == "*") {
        if (varInputs != nullptr) {
            auto colNames = varInputs->getColNames();
            for (auto &prop : colNames) {
                auto *alias = new std::string(*(variableExpr->alias()));
                Expression *expr =
                        new VariablePropertyExpression(alias, new std::string(prop));
                YieldColumn *column = new YieldColumn(expr);
                yieldColsHolder_->addColumn(column);
                yields.emplace_back(column);
            }
        } else {
            // should not reach here.
            return Status::Error("Variable `%s' is nullptr.", varname->c_str());
        }
        return true;
    }
    return false;
}
}   // namespace graph
}   // namespace nebula
