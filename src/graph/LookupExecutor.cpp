/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/LookupExecutor.h"
#include <interface/gen-cpp2/common_types.h>

namespace nebula {
namespace graph {

LookupExecutor::LookupExecutor(Sentence* sentence, ExecutionContext* ectx)
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
    auto* mc = ectx()->getMetaClient();
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
    auto* clause = sentence_->whereClause();
    if (!clause) {
        return Status::SyntaxError("Where clause is required");
    }
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);
    return Status::OK();
}

Status LookupExecutor::prepareYield() {
    auto* clause = sentence_->yieldClause();
    if (clause != nullptr) {
        auto schema = (isEdge_) ? ectx_->schemaManager()->getEdgeSchema(spaceId_, tagOrEdge_)
                                : ectx_->schemaManager()->getTagSchema(spaceId_, tagOrEdge_);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << from_->c_str();
            return Status::Error("No schema found for `%s'", from_->c_str());
        }
        yieldClauseWrapper_ = std::make_unique<YieldClauseWrapper>(clause);
        auto* varHolder = ectx()->variableHolder();
        auto status = yieldClauseWrapper_->prepare(inputs_.get(), varHolder, yields_);
        if (!status.ok()) {
            return status;
        }
        for (auto* col : yields_) {
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
        status = findOptimalIndex();
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

Status LookupExecutor::traversalExpr(const Expression* expr, const meta::SchemaProviderIf* schema) {
    switch (expr->kind()) {
        case nebula::Expression::kLogical: {
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
        case nebula::Expression::kRelational: {
            std::string prop;
            VariantType v;
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto ret = relationalExprCheck(rExpr->op());
            if (!ret.ok()) {
                return ret;
            }
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            if (left->kind() == nebula::Expression::kAliasProp &&
                right->kind() == nebula::Expression::kAliasProp) {
                return Status::SyntaxError("Does not support left and right are both property");
            } else if (left->kind() == nebula::Expression::kAliasProp) {
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

            if (rExpr->op() != RelationalExpression::Operator::EQ &&
                rExpr->op() != RelationalExpression::Operator::NE) {
                auto type = schema->getFieldType(prop).type;
                if (!supportedDataTypeForRange(type)) {
                    return Status::SyntaxError("Data type of field %s not support range scan",
                                               prop.c_str());
                }
            }
            break;
        }
        case nebula::Expression::kFunctionCall: {
            return Status::SyntaxError("Function expressions are not supported yet");
        }
        default: {
            return Status::SyntaxError("Syntax error ： %s", expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status LookupExecutor::checkFilter() {
    auto* sm = ectx()->schemaManager();
    auto schema =
        isEdge_ ? sm->getEdgeSchema(spaceId_, tagOrEdge_) : sm->getTagSchema(spaceId_, tagOrEdge_);
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

Status LookupExecutor::findOptimalIndex() {
    // The rule of priority is '==' --> '< > <= >=' --> '!='
    // Step 1 : find out all valid indexes for where condition.
    auto validIndexes = findValidIndex();
    if (validIndexes.empty()) {
        LOG(ERROR) << "No valid index found";
        return Status::IndexNotFound();
    }
    // Step 2 : find optimal indexes for equal condition.
    auto indexesEq = findIndexForEqualScan(validIndexes);
    if (indexesEq.size() == 1) {
        index_ = indexesEq[0]->get_index_id();
        return Status::OK();
    }
    // Step 3 : find optimal indexes for range condition.
    auto indexesRange = findIndexForRangeScan(indexesEq);

    // At this stage, all the optimizations are done.
    // Because the storage layer only needs one. So return first one of indexesRange.
    index_ = indexesRange[0]->get_index_id();
    return Status::OK();
}

std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> LookupExecutor::findValidIndexWithStr() {
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> validIndexes;
    // Because the string type is a variable-length field,
    // the WHERE condition must cover all fields in index for performance.

    // Maybe there are duplicate fields in the WHERE condition,
    // so need to using std::set remove duplicate field at here, for example :
    // where col1 > 1 and col1 < 5, the field col1 will appear twice in filters_.
    std::set<std::string> cols;
    for (const auto& filter : filters_) {
        cols.emplace(filter.first);
    }
    for (const auto& index : indexes_) {
        if (index->get_fields().size() != cols.size()) {
            continue;
        }
        bool allColsHint = true;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(cols.begin(), cols.end(), [field](const auto& col) {
                return field.get_name() == col;
            });
            if (it == cols.end()) {
                allColsHint = false;
                break;
            }
        }
        if (allColsHint) {
            validIndexes.emplace_back(index);
        }
    }
    if (validIndexes.empty()) {
        LOG(WARNING) << "The WHERE condition contains fields of string type, "
                     << "So the WHERE condition must cover all fields in index.";
    }
    return validIndexes;
}

std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> LookupExecutor::findValidIndexNoStr() {
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> validIndexes;
    // Find indexes for match all fields by where condition.
    // Non-string type fields do not need to involve all fields
    for (const auto& index : indexes_) {
        bool allColsHint = true;
        const auto& fields = index->get_fields();
        // If index including string type fields, skip this index.
        auto stringField = std::find_if(fields.begin(), fields.end(), [](const auto& f) {
            return f.get_type().get_type() == nebula::cpp2::SupportedType::STRING;
        });
        if (stringField != fields.end()) {
            continue;
        }
        for (const auto& filter : filters_) {
            auto it = std::find_if(fields.begin(), fields.end(), [filter](const auto& field) {
                return field.get_name() == filter.first;
            });
            if (it == fields.end()) {
                allColsHint = false;
                break;
            }
        }
        if (allColsHint) {
            validIndexes.emplace_back(index);
        }
    }
    // If the first field of the index does not match any condition, the index is invalid.
    // remove it from validIndexes.
    if (!validIndexes.empty()) {
        auto index = validIndexes.begin();
        while (index != validIndexes.end()) {
            const auto& fields = index->get()->get_fields();
            auto it = std::find_if(filters_.begin(), filters_.end(), [fields](const auto& filter) {
                return filter.first == fields[0].get_name();
            });
            if (it == filters_.end()) {
                validIndexes.erase(index);
            } else {
                index++;
            }
        }
    }
    return validIndexes;
}

std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> LookupExecutor::findValidIndex() {
    // Check contains string type field from where condition
    auto* sm = ectx()->schemaManager();
    auto schema =
        isEdge_ ? sm->getEdgeSchema(spaceId_, tagOrEdge_) : sm->getTagSchema(spaceId_, tagOrEdge_);
    if (schema == nullptr) {
        LOG(ERROR) << "No schema found : " << from_;
        return {};
    }

    // Check conditions whether contains string type for where condition.
    // Different optimization rules：
    // Contains string type : Conditions need to match all the index columns. for example
    //                         where c1 == 'a' and c2 == 'b'
    //                         index1 (c1, c2) is valid
    //                         index2 (c1, c2, c3) is invalid.
    //                         so index1 should be hit.
    //
    // Not contains string type : Conditions only needs to match the first N columns of the index.
    //                            for example : where c1 == 1 and c2 == 2
    //                            index1 (c1, c2) is valid.
    //                            index2 (c1, c2, c3) is valid too.
    //                            so index1 and index2 should be hit.
    bool hasStringCol = false;
    for (const auto& filter : filters_) {
        auto type = schema->getFieldType(filter.first);
        if (type.get_type() == nebula::cpp2::SupportedType::STRING) {
            hasStringCol = true;
            break;
        }
    }
    return hasStringCol ? findValidIndexWithStr() : findValidIndexNoStr();
}

std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> LookupExecutor::findIndexForEqualScan(
    const std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>& indexes) {
    std::vector<std::pair<int32_t, std::shared_ptr<nebula::cpp2::IndexItem>>> eqIndexHint;
    for (auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(filters_.begin(), filters_.end(), [field](const auto& filter) {
                return filter.first == field.get_name();
            });
            if (it == filters_.end()) {
                break;
            }
            if (it->second == RelationalExpression::Operator::EQ) {
                ++hintCount;
            } else {
                break;
            }
        }
        eqIndexHint.emplace_back(hintCount, index);
    }
    // Sort the priorityIdxs for equivalent condition.
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> priorityIdxs;
    auto comp = [](std::pair<int32_t, std::shared_ptr<nebula::cpp2::IndexItem>>& lhs,
                   std::pair<int32_t, std::shared_ptr<nebula::cpp2::IndexItem>>& rhs) {
        return lhs.first > rhs.first;
    };
    std::sort(eqIndexHint.begin(), eqIndexHint.end(), comp);
    // Get the index with the highest hit rate from eqIndexHint.
    int32_t maxHint = eqIndexHint[0].first;
    for (const auto& hint : eqIndexHint) {
        if (hint.first < maxHint) {
            break;
        }
        priorityIdxs.emplace_back(hint.second);
    }
    return priorityIdxs;
}

std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> LookupExecutor::findIndexForRangeScan(
    const std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>& indexes) {
    std::map<int32_t, std::shared_ptr<nebula::cpp2::IndexItem>> rangeIndexHint;
    for (const auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto fi = std::find_if(filters_.begin(), filters_.end(), [field](const auto& rel) {
                return rel.first == field.get_name();
            });
            if (fi == filters_.end()) {
                break;
            }
            if (fi->second == RelationalExpression::Operator::EQ) {
                continue;
            }
            if (fi->second == RelationalExpression::Operator::GE ||
                fi->second == RelationalExpression::Operator::GT ||
                fi->second == RelationalExpression::Operator::LE ||
                fi->second == RelationalExpression::Operator::LT) {
                hintCount++;
            } else {
                break;
            }
        }
        rangeIndexHint[hintCount] = index;
    }
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> priorityIdxs;
    int32_t maxHint = rangeIndexHint.rbegin()->first;
    for (auto iter = rangeIndexHint.rbegin(); iter != rangeIndexHint.rend(); iter++) {
        if (iter->first < maxHint) {
            break;
        }
        priorityIdxs.emplace_back(iter->second);
    }
    return priorityIdxs;
}

void LookupExecutor::lookUp() {
    auto* sc = ectx()->getStorageClient();
    auto filter = Expression::encode(sentence_->whereClause()->filter());
    auto future = sc->lookUpIndex(spaceId_, index_, filter, returnCols_, isEdge_);
    auto* runner = ectx()->rctx()->runner();
    auto cb = [this](auto&& result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Lookup vertices failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Lookup partially failed: " << completeness << "%";
            for (auto& error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            ectx()->addWarningMsg("Lookup executor was partially performed");
        }
        finishExecution(std::forward<decltype(result)>(result));
    };
    auto error = [this](auto&& e) {
        LOG(ERROR) << "Exception when handle lookup: " << e.what();
        doError(Status::Error("Exception when handle lookup: %s.", e.what().c_str()));
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

void LookupExecutor::setupResponse(cpp2::ExecutionResponse& resp) {
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
    for (auto* col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->expr()->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}

void LookupExecutor::finishExecution(RpcResponse&& resp) {
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

bool LookupExecutor::setupInterimResult(RpcResponse&& resp,
                                        std::unique_ptr<InterimResult>& result) {
    // Generic results
    result = std::make_unique<InterimResult>(getResultColumnNames());
    std::shared_ptr<SchemaWriter> schema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto cb = [&](std::vector<VariantType> record,
                  const std::vector<nebula::cpp2::SupportedType>& colTypes) -> Status {
        if (schema == nullptr) {
            schema = std::make_shared<SchemaWriter>();
            auto colnames = getResultColumnNames();
            if (record.size() != colTypes.size()) {
                LOG(ERROR) << "Record size: " << record.size()
                           << " != column type size: " << colTypes.size();
                return Status::Error("Record size is not equal to column type size, [%lu != %lu]",
                                     record.size(),
                                     colTypes.size());
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
            }   // for
            rsWriter = std::make_unique<RowSetWriter>(schema);
        }   // if

        RowWriter writer(schema);
        for (auto& column : record) {
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
    };   // cb
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

StatusOr<std::vector<cpp2::RowValue>> LookupExecutor::toThriftResponse(RpcResponse&& response) {
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
    auto cb = [&](std::vector<VariantType> record,
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
                default: {
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
                } break;
            }
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        return Status::OK();
    };   // cb

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

bool LookupExecutor::processFinalEdgeResult(RpcResponse& rpcResp, const Callback& cb) const {
    auto& all = rpcResp.responses();
    std::vector<nebula::cpp2::SupportedType> colTypes;
    std::vector<VariantType> record;
    record.reserve(returnCols_.size() + 3);
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto& resp : all) {
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

bool LookupExecutor::processFinalVertexResult(RpcResponse& rpcResp, const Callback& cb) const {
    auto& all = rpcResp.responses();
    std::vector<nebula::cpp2::SupportedType> colTypes;
    std::vector<VariantType> record;
    record.reserve(returnCols_.size() + 1);
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto& resp : all) {
        if (!resp.__isset.vertices || resp.get_vertices() == nullptr ||
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
        case RelationalExpression::Operator::EQ:
        case RelationalExpression::Operator::GE:
        case RelationalExpression::Operator::GT:
        case RelationalExpression::Operator::LE:
        case RelationalExpression::Operator::LT:
        case RelationalExpression::Operator::NE: {
            return Status::OK();
        }
        case RelationalExpression::Operator::CONTAINS: {
            return Status::SyntaxError("Unsupported 'CONTAINS' in where clause");
        }
    }
    return Status::OK();
}

bool LookupExecutor::supportedDataTypeForRange(nebula::cpp2::SupportedType type) const {
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
}   // namespace graph
}   // namespace nebula
