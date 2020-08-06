/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/LookupExecutor.h"
#include <interface/gen-cpp2/common_types.h>

namespace nebula {
namespace graph {

LookupExecutor::LookupExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "lookup") {
    sentence_ = static_cast<LookupSentence*>(sentence);
}

Status LookupExecutor::prepare() {
    return Status::OK();
}

Status LookupExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    Status status = Status::OK();
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        status = prepareFrom();
        if (!status.ok()) {
            break;
        }
        status = prepareIndexes();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
        status = prepareYield();
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

Status LookupExecutor::prepareFrom() {
    spaceId_ = ectx()->rctx()->session()->space();
    from_ = sentence_->from();
    auto *mc = ectx()->getMetaClient();
    auto tagResult = mc->getTagIDByNameFromCache(spaceId_, *from_);
    if (tagResult.ok()) {
        tagOrEdge_ = tagResult.value();
        return Status::OK();
    }
    auto edgeResult = mc->getEdgeTypeByNameFromCache(spaceId_, *from_);
    if (edgeResult.ok()) {
        tagOrEdge_ = edgeResult.value();
        isEdge_ = true;
        return Status::OK();
    }
    LOG(ERROR) << "Tag or Edge was not found : " << from_->c_str();
    return Status::Error("Tag or Edge was not found : %s", from_->c_str());
}

Status LookupExecutor::prepareIndexes() {
    auto ret = (isEdge_) ? ectx()->getMetaClient()->getEdgeIndexesFromCache(spaceId_)
                         : ectx()->getMetaClient()->getTagIndexesFromCache(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << "No index was found";
        return Status::IndexNotFound();
    }

    auto indexes = ret.value();
    if (indexes.empty()) {
        return Status::IndexNotFound();
    }
    for (auto& index : indexes) {
        if (index->get_schema_name() == *from_) {
            indexes_.emplace_back(index);
        }
    }
    if (indexes_.empty()) {
        LOG(ERROR) << "No index was found";
        return Status::IndexNotFound();
    }
    return Status::OK();
}

Status LookupExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    if (!clause) {
        return Status::SyntaxError("Where clause is required");
    }
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);
    return Status::OK();
}

Status LookupExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        auto schema = (isEdge_) ? ectx_->schemaManager()->getEdgeSchema(spaceId_, tagOrEdge_)
                                : ectx_->schemaManager()->getTagSchema(spaceId_, tagOrEdge_);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << from_->c_str();
            return Status::Error("No schema found for `%s'", from_->c_str());
        }
        yieldClauseWrapper_ = std::make_unique<YieldClauseWrapper>(clause);
        auto *varHolder = ectx()->variableHolder();
        auto status = yieldClauseWrapper_->prepare(inputs_.get(), varHolder, yields_);
        if (!status.ok()) {
            return status;
        }
        for (auto *col : yields_) {
            if (!col->getFunName().empty()) {
                return Status::SyntaxError("Not supported yet");
            }
            auto* prop = col->expr();
            if (prop->kind() != Expression::kAliasProp) {
                return Status::SyntaxError("Expressions other than AliasProp are not supported");
            }
            auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(prop);
            auto st = checkAliasProperty(aExpr);
            if (!st.ok()) {
                return st;
            }
            if (schema->getFieldIndex(aExpr->prop()->c_str()) < 0) {
                LOG(ERROR) << "Unknown column " << aExpr->prop()->c_str();
                return Status::Error("Unknown column `%s' in schema", aExpr->prop()->c_str());
            }
            returnCols_.emplace_back(aExpr->prop()->c_str());
        }
    }
    return Status::OK();
}

Status LookupExecutor::checkAliasProperty(const AliasPropertyExpression* aExpr) {
    if (*aExpr->alias() != *from_) {
        return Status::SyntaxError("Edge or Tag name error : %s ", aExpr->alias()->c_str());
    }
    return Status::OK();
}

Status LookupExecutor::optimize() {
    Status status = Status::OK();
    do {
        status = checkFilter();
        if (!status.ok()) {
            break;
        }
        status = findValidIndex();
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

Status LookupExecutor::traversalExpr(const Expression *expr, const meta::SchemaProviderIf* schema) {
    switch (expr->kind()) {
        case nebula::Expression::kLogical : {
            Status ret = Status::OK();
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            if (lExpr->op() == LogicalExpression::Operator::XOR ||
                lExpr->op() == LogicalExpression::Operator::OR) {
                return Status::SyntaxError("OR and XOR are not supported "
                                           "in lookup where clause ：%s",
                                           lExpr->toString().c_str());
            }
            auto* left = lExpr->left();
            ret = traversalExpr(left, schema);
            if (!ret.ok()) {
                return ret;
            }
            auto* right = lExpr->right();
            ret = traversalExpr(right, schema);
            if (!ret.ok()) {
                return ret;
            }
            break;
        }
        case nebula::Expression::kRelational : {
            std::string prop;
            VariantType v;
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto ret = relationalExprCheck(rExpr->op());
            if (!ret.ok()) {
                return ret;
            }
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            /**
             *  TODO (sky) : Does not support left expr and right expr are both kAliasProp.
             */
            if (left->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(left);
                auto st = checkAliasProperty(aExpr);
                if (!st.ok()) {
                    return st;
                }
                prop = *aExpr->prop();
                filters_.emplace_back(std::make_pair(prop, rExpr->op()));
            } else if (right->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(right);
                auto st = checkAliasProperty(aExpr);
                if (!st.ok()) {
                    return st;
                }
                prop = *aExpr->prop();
                filters_.emplace_back(std::make_pair(prop, rExpr->op()));
            } else {
                return Status::SyntaxError("Unsupported expression ：%s",
                                           rExpr->toString().c_str());
            }

            if (rExpr->op() != RelationalExpression::Operator::EQ) {
                auto type = schema->getFieldType(prop).type;
                if (!dataTypeCheckForRange(type)) {
                    return Status::SyntaxError("Data type of field %s not support range scan",
                                               prop.c_str());
                }
            }
            break;
        }
        case nebula::Expression::kFunctionCall : {
            return Status::SyntaxError("Function expressions are not supported yet");
        }
        default : {
            return Status::SyntaxError("Syntax error ： %s", expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status LookupExecutor::checkFilter() {
    auto *sm = ectx()->schemaManager();
    auto schema = isEdge_
                  ? sm->getEdgeSchema(spaceId_, tagOrEdge_)
                  : sm->getTagSchema(spaceId_, tagOrEdge_);
    if (schema == nullptr) {
        return Status::Error("No schema found %s", from_->c_str());
    }

    auto status = traversalExpr(sentence_->whereClause()->filter(), schema.get());
    if (!status.ok()) {
        return status;
    }
    if (filters_.empty()) {
        return Status::SyntaxError("Where clause error . have not index matching");
    }
    return Status::OK();
}

Status
LookupExecutor::findValidIndex() {
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> indexes;
    std::set<std::string> filterCols;
    for (auto& filter : filters_) {
        filterCols.insert(filter.first);
    }
    /**
     * step 1 : found out all valid indexes. for example :
     * tag (col1 , col2, col3)
     * index1 on tag (col1, col2)
     * index2 on tag (col2, col1)
     * index3 on tag (col3)
     *
     * where where clause is below :
     * col1 > 1 and col2 > 1 --> index1 and index2 are valid.
     * col1 > 1 --> index1 is valid.
     * col2 > 1 --> index2 is valid.
     * col3 > 1 --> index3 is valid.
     */
    for (auto& index : indexes_) {
        bool matching = true;
        size_t filterNum = 1;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(filterCols.begin(), filterCols.end(),
                                   [field](const auto &name) {
                                       return field.get_name() == name;
                                   });
            if (it == filterCols.end()) {
                matching = false;
                break;
            }
            if (filterNum++ == filterCols.size()) {
                break;
            }
        }
        if (!matching || index->get_fields().size() < filterCols.size()) {
            continue;
        }
        indexes.emplace_back(index);
    }

    if (indexes.empty()) {
        return Status::IndexNotFound();
    }

    /**
     * step 2 , if have multiple valid indexes, get the best one.
     * for example : if where clause is :
     * col1 > 1 and col2 > 1 --> index1 and index2 are valid. get one of these at random.
     * col1 > 1 and col2 == 1 --> index1 and index2 are valid.
     *                            but need choose one for storage layer.
     *                            here index2 is chosen because col2 have a equivalent value.
     */
    std::map<int32_t, IndexID> indexHint;
    for (auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(filters_.begin(), filters_.end(),
                                   [field](const auto &rel) {
                                       return rel.second == RelationalExpression::EQ;
                                   });
            if (it == filters_.end()) {
                break;
            }
            ++hintCount;
        }
        indexHint[hintCount] = index->get_index_id();
    }
    index_ = indexHint.rbegin()->second;
    return Status::OK();
}

void LookupExecutor::lookUp() {
    auto *sc = ectx()->getStorageClient();
    auto filter = Expression::encode(sentence_->whereClause()->filter());
    auto future  = sc->lookUpIndex(spaceId_, index_, filter, returnCols_, isEdge_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Lookup vertices failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Lookup partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            ectx()->addWarningMsg("Lookup executor was partially performed");
        }
        finishExecution(std::forward<decltype(result)>(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when handle lookup: " << e.what();
        doError(Status::Error("Exception when handle lookup: %s.",
                              e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void LookupExecutor::execute() {
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    status = optimize();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    lookUp();
}

void LookupExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    inputs_ = std::move(result);
}

void LookupExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

std::vector<std::string> LookupExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yields_.size() + ((isEdge_) ? 3 : 1));
    if (isEdge_) {
        result.emplace_back("SrcVID");
        result.emplace_back("DstVID");
        result.emplace_back("Ranking");
    } else {
        result.emplace_back("VertexID");
    }
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->expr()->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}

void LookupExecutor::finishExecution(RpcResponse &&resp) {
    if (onResult_) {
        std::unique_ptr<InterimResult> outputs;
        if (!setupInterimResult(std::move(resp), outputs)) {
            return;
        }
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(getResultColumnNames());
        auto ret = toThriftResponse(std::forward<RpcResponse>(resp));
        if (!ret.ok()) {
            LOG(ERROR) << "Get rows failed: " << ret.status();
            return;
        }
        if (!ret.value().empty()) {
            resp_->set_rows(std::move(ret).value());
        }
    }
    doFinish(Executor::ProcessControl::kNext);
}

bool LookupExecutor::setupInterimResult(RpcResponse &&resp,
                                        std::unique_ptr<InterimResult> &result) {
    // Generic results
    result = std::make_unique<InterimResult>(getResultColumnNames());
    std::shared_ptr<SchemaWriter> schema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto cb = [&] (std::vector<VariantType> record,
                   const std::vector<nebula::cpp2::SupportedType>& colTypes) -> Status {
        if (schema == nullptr) {
            schema = std::make_shared<SchemaWriter>();
            auto colnames = getResultColumnNames();
            if (record.size() != colTypes.size()) {
                LOG(ERROR) << "Record size: " << record.size()
                           << " != column type size: " << colTypes.size();
                return Status::Error("Record size is not equal to column type size, [%lu != %lu]",
                                     record.size(), colTypes.size());
            }
            for (auto i = 0u; i < record.size(); i++) {
                nebula::cpp2::SupportedType type;
                if (colTypes[i] == nebula::cpp2::SupportedType::UNKNOWN) {
                    switch (record[i].which()) {
                        case VAR_INT64:
                            // all integers in InterimResult are regarded as type of INT
                            type = nebula::cpp2::SupportedType::INT;
                            break;
                        case VAR_DOUBLE:
                            type = nebula::cpp2::SupportedType::DOUBLE;
                            break;
                        case VAR_BOOL:
                            type = nebula::cpp2::SupportedType::BOOL;
                            break;
                        case VAR_STR:
                            type = nebula::cpp2::SupportedType::STRING;
                            break;
                        default:
                            LOG(ERROR) << "Unknown VariantType: " << record[i].which();
                            return Status::Error("Unknown VariantType: %d", record[i].which());
                    }
                } else {
                    type = colTypes[i];
                }
                schema->appendCol(colnames[i], type);
            }  // for
            rsWriter = std::make_unique<RowSetWriter>(schema);
        }  // if

        RowWriter writer(schema);
        for (auto &column : record) {
            switch (column.which()) {
                case VAR_INT64:
                    writer << boost::get<int64_t>(column);
                    break;
                case VAR_DOUBLE:
                    writer << boost::get<double>(column);
                    break;
                case VAR_BOOL:
                    writer << boost::get<bool>(column);
                    break;
                case VAR_STR:
                    writer << boost::get<std::string>(column);
                    break;
                default:
                    LOG(ERROR) << "Unknown VariantType: " << column.which();
                    return Status::Error("Unknown VariantType: %d", column.which());
            }
        }

        rsWriter->addRow(writer.encode());
        return Status::OK();
    };  // cb
    if (isEdge_) {
        if (!processFinalEdgeResult(resp, cb)) {
            return false;
        }
    } else {
        if (!processFinalVertexResult(resp, cb)) {
            return false;
        }
    }


    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return true;
}

StatusOr<std::vector<cpp2::RowValue>>
LookupExecutor::toThriftResponse(RpcResponse&& response) {
    std::vector<cpp2::RowValue> rows;
    int64_t totalRows = 0;
    if (isEdge_) {
        for (auto& resp : response.responses()) {
            if (resp.__isset.edges) {
                totalRows += resp.get_edges()->size();
            }
        }
    } else {
        for (auto& resp : response.responses()) {
            if (resp.__isset.vertices) {
                totalRows += resp.get_vertices()->size();
            }
        }
    }

    rows.reserve(totalRows);
    auto cb = [&] (std::vector<VariantType> record,
                   const std::vector<nebula::cpp2::SupportedType>& colTypes) -> Status {
        std::vector<cpp2::ColumnValue> row;
        row.reserve(record.size());
        for (size_t i = 0; i < colTypes.size(); i++) {
            auto& column = record[i];
            auto& type = colTypes[i];
            row.emplace_back();
            switch (type) {
                case nebula::cpp2::SupportedType::BOOL:
                    row.back().set_bool_val(boost::get<bool>(column));
                    break;
                case nebula::cpp2::SupportedType::INT:
                    row.back().set_integer(boost::get<int64_t>(column));
                    break;
                case nebula::cpp2::SupportedType::DOUBLE:
                    row.back().set_double_precision(boost::get<double>(column));
                    break;
                case nebula::cpp2::SupportedType::FLOAT:
                    row.back().set_single_precision(boost::get<double>(column));
                    break;
                case nebula::cpp2::SupportedType::STRING:
                    row.back().set_str(boost::get<std::string>(column));
                    break;
                case nebula::cpp2::SupportedType::TIMESTAMP:
                    row.back().set_timestamp(boost::get<int64_t>(column));
                    break;
                case nebula::cpp2::SupportedType::VID:
                    row.back().set_id(boost::get<int64_t>(column));
                    break;
                default:
                {
                    switch (column.which()) {
                        case VAR_INT64:
                            row.back().set_integer(boost::get<int64_t>(column));
                            break;
                        case VAR_DOUBLE:
                            row.back().set_double_precision(boost::get<double>(column));
                            break;
                        case VAR_BOOL:
                            break;
                        case VAR_STR:
                            row.back().set_str(boost::get<std::string>(column));
                            break;
                        default:
                            LOG(FATAL) << "Unknown VariantType: " << column.which();
                    }
                }
                    break;
            }
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        return Status::OK();
    };  // cb

    if (isEdge_) {
        if (!processFinalEdgeResult(response, cb)) {
            return Status::Error("process failed");
        }
    } else {
        if (!processFinalVertexResult(response, cb)) {
            return Status::Error("process failed");
        }
    }
    return rows;
}

bool LookupExecutor::processFinalEdgeResult(RpcResponse &rpcResp, const Callback& cb) const {
    auto& all = rpcResp.responses();
    std::vector<nebula::cpp2::SupportedType> colTypes;
    std::vector<VariantType> record;
    record.reserve(returnCols_.size() + 3);
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto &resp : all) {
        if (!resp.__isset.edges || resp.get_edges() == nullptr || resp.get_edges()->empty()) {
            continue;
        }
        if (colTypes.empty()) {
            colTypes.emplace_back(nebula::cpp2::SupportedType::VID);
            colTypes.emplace_back(nebula::cpp2::SupportedType::VID);
            colTypes.emplace_back(nebula::cpp2::SupportedType::INT);
            if (!returnCols_.empty()) {
                schema = std::make_shared<ResultSchemaProvider>(*(resp.get_schema()));
                std::for_each(returnCols_.begin(), returnCols_.end(), [&](auto& col) {
                    colTypes.emplace_back(schema->getFieldType(col).get_type());
                });
            }
        }
        for (const auto& data : *resp.get_edges()) {
            const auto& edge = data.get_key();
            record.emplace_back(edge.get_src());
            record.emplace_back(edge.get_dst());
            record.emplace_back(edge.get_ranking());
            for (auto& column : returnCols_) {
                auto reader = RowReader::getRowReader(data.get_props(), schema);
                auto res = RowReader::getPropByName(reader.get(), column);
                if (!ok(res)) {
                    LOG(ERROR) << "Skip the bad value for prop " << column;
                    continue;
                }
                auto&& v = value(std::move(res));
                record.emplace_back(std::move(v));
            }
            auto cbStatus = cb(std::move(record), colTypes);
            if (!cbStatus.ok()) {
                LOG(ERROR) << cbStatus;
                doError(std::move(cbStatus));
                return false;
            }
        }
    }   // for `resp'
    return true;
}

bool LookupExecutor::processFinalVertexResult(RpcResponse &rpcResp,
                                              const Callback& cb) const {
    auto& all = rpcResp.responses();
    std::vector<nebula::cpp2::SupportedType> colTypes;
    std::vector<VariantType> record;
    record.reserve(returnCols_.size() + 1);
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto &resp : all) {
        if (!resp.__isset.vertices ||
            resp.get_vertices() == nullptr ||
            resp.get_vertices()->empty()) {
            continue;
        }
        if (colTypes.empty()) {
            colTypes.emplace_back(nebula::cpp2::SupportedType::VID);
            if (!returnCols_.empty()) {
                schema = std::make_shared<ResultSchemaProvider>(*(resp.get_schema()));
                std::for_each(returnCols_.begin(), returnCols_.end(), [&](auto& col) {
                    colTypes.emplace_back(schema->getFieldType(col).get_type());
                });
            }
        }
        for (const auto& data : *resp.get_vertices()) {
            const auto& vertexId = data.get_vertex_id();
            record.emplace_back(vertexId);
            for (auto& column : returnCols_) {
                auto reader = RowReader::getRowReader(data.get_props(), schema);
                auto res = RowReader::getPropByName(reader.get(), column);
                if (!ok(res)) {
                    LOG(ERROR) << "Skip the bad value for prop " << column;
                    continue;
                }
                auto&& v = value(std::move(res));
                record.emplace_back(v);
            }
            auto cbStatus = cb(std::move(record), colTypes);
            if (!cbStatus.ok()) {
                LOG(ERROR) << cbStatus;
                doError(std::move(cbStatus));
                return false;
            }
        }
    }   // for `resp'
    return true;
}

Status LookupExecutor::relationalExprCheck(RelationalExpression::Operator op) const {
    // Compile will fail after added new relational operations.
    // Need to consider the logic in method 'traversalExpr' after new relational operation added.
    switch (op) {
        case RelationalExpression::Operator::EQ :
        case RelationalExpression::Operator::GE :
        case RelationalExpression::Operator::GT :
        case RelationalExpression::Operator::LE :
        case RelationalExpression::Operator::LT : {
            return Status::OK();
        }
        case RelationalExpression::Operator::NE :
        case RelationalExpression::Operator::CONTAINS : {
            return Status::SyntaxError("Unsupported operator '!=' or 'CONTAINS' in where clause");
        }
    }
    return Status::OK();
}

bool LookupExecutor::dataTypeCheckForRange(nebula::cpp2::SupportedType type) const {
    switch (type) {
        case nebula::cpp2::SupportedType::INT:
        case nebula::cpp2::SupportedType::DOUBLE:
        case nebula::cpp2::SupportedType::FLOAT:
        case nebula::cpp2::SupportedType::TIMESTAMP:
            return true;
        default:
            return false;
    }
}
}  // namespace graph
}  // namespace nebula
