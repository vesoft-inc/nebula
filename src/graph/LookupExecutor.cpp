/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/LookupExecutor.h"

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
    spaceId_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        status = prepareFrom();
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
    return Status::Error("Tag or Edge was not found : %s", from_->c_str());
}

Status LookupExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    whereWrapper_ = std::make_unique<WhereWrapper>(clause);
    auto status = whereWrapper_->prepare(expCtx_.get());
    return status;
}

Status LookupExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        auto schema = (isEdge_) ? ectx_->schemaManager()->getEdgeSchema(spaceId_, tagOrEdge_)
                                : ectx_->schemaManager()->getTagSchema(spaceId_, tagOrEdge_);
        if (schema == nullptr) {
            return Status::Error("No tag schema for %s", from_->c_str());
        }
        yieldClauseWrapper_ = std::make_unique<YieldClauseWrapper>(clause);
        auto *varHolder = ectx()->variableHolder();
        auto status = yieldClauseWrapper_->prepare(inputs_.get(), varHolder, yields_);
        if (!status.ok()) {
            return status;
        }
        for (auto *col : yields_) {
            if (!col->getFunName().empty()) {
                return Status::SyntaxError("Do not support in aggregated query");
            }
            auto* prop = col->expr();
            if (prop->kind() != Expression::kAliasProp) {
                return Status::SyntaxError("Expressions other than AliasProp are not supported");
            }
            auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(prop);
            if (schema->getFieldIndex(aExpr->prop()->c_str()) < 0) {
                return Status::Error("Unknown column `%s' in schema", aExpr->prop()->c_str());
            }
            returnCols_.emplace_back(aExpr->prop()->c_str());
        }
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
        status = chooseIndex();
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

Status LookupExecutor::traversalExpr(const Expression *expr) {
    switch (expr->kind()) {
        case nebula::Expression::kLogical : {
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            if (lExpr->op() == LogicalExpression::Operator::XOR) {
                return Status::SyntaxError("Unsupported syntax ：%s", lExpr->toString().c_str());
            } else if (lExpr->op() == LogicalExpression::Operator::OR) {
                skipOptimize_ = true;
            }
            auto* left = lExpr->left();
            traversalExpr(left);
            auto* right = lExpr->right();
            traversalExpr(right);
            break;
        }
        case nebula::Expression::kRelational : {
            std::string prop;
            VariantType v;
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            if (left->kind() == nebula::Expression::kAliasProp
                && right->kind() == nebula::Expression::kPrimary) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(left);
                prop = *aExpr->prop();
                rels_.emplace_back(std::make_pair(prop, rExpr->op()));
            } else if (left->kind() == nebula::Expression::kPrimary
                       && right->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(right);
                prop = *aExpr->prop();
                rels_.emplace_back(std::make_pair(prop, rExpr->op()));
            } else {
                return Status::SyntaxError("Unsupported syntax ：%s", rExpr->toString().c_str());
            }
            break;
        }
        case nebula::Expression::kFunctionCall : {
            skipOptimize_ = true;
            break;
        }
        default : {
            return Status::SyntaxError("Unsupported syntax ： %s", expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status LookupExecutor::checkFilter() {
    auto status = traversalExpr(whereWrapper_->getFilter());
    return status;
}

Status LookupExecutor::chooseIndex() {
    auto ret = (isEdge_) ? ectx()->getMetaClient()->getEdgeIndexesFromCache(spaceId_)
                         : ectx()->getMetaClient()->getTagIndexesFromCache(spaceId_);
    if (!ret.ok()) {
        return Status::IndexNotFound();
    }

    auto indexes = ret.value();
    if (indexes.empty()) {
        return Status::IndexNotFound();
    }

    auto validIndexes = findValidIndex(indexes);
    if (validIndexes.empty()) {
        return Status::IndexNotFound("No matching index was found");
    }
    return findBestIndex(validIndexes);
}

/**
 * Choose the index that filter best matches, for example :
 * the filter is col1 == 1 and col2 == 2 and col3 == 3,
 * the indexes are index1 (col4, col3, col2, col1)
 *                 index2 (col1, col2, col3, col4)
 *                 index3 (col1, colA, col2, col3)
 * as above, these indexes all are valid, but the index2 should be choose.
 */
Status LookupExecutor::findBestIndex(std::vector<IndexID> ids) {
    Status status = Status::OK();
    if (ids.size() == 1) {
        index_ = ids[0];
        return status;
    }
    /**
     * Returns the first index if no optimization is required
     */
    if (skipOptimize_) {
        index_ = ids[0];
        return status;
    }
    auto *mc = ectx()->getMetaClient();

    /**
     * std::map<column_hint_count, IndexID>
     */
    std::map<int32_t, IndexID> indexHint;
    for (auto& indexId : ids) {
        auto itemStatus = (isEdge_) ? mc->getEdgeIndexFromCache(spaceId_, indexId)
                                    : mc->getTagIndexFromCache(spaceId_, indexId);
        if (!itemStatus.ok()) {
            return itemStatus.status();
        }
        const auto& index = itemStatus.value();
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(rels_.begin(), rels_.end(),
                                   [field](const auto &rel) {
                                       return field.get_name() == rel.first &&
                                              rel.second == RelationalExpression::EQ;
                                   });
            if (it == rels_.end()) {
                break;
            }
            ++hintCount;
        }
        indexHint[hintCount] = indexId;
    }
    index_ = indexHint.rbegin()->second;
    return status;
}

std::vector<IndexID>
LookupExecutor::findValidIndex(const std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>& ids) {
    std::vector<IndexID> indexes;
    for (auto& index : ids) {
        bool indexHint = true;
        if (index->get_schema_name() != *from_) {
            continue;
        }
        const auto& fields = index->get_fields();
        for (auto& col : rels_) {
            auto it = std::find_if(fields.begin(), fields.end(),
                                   [col](const auto &field) {
                                       return field.get_name() == col.first;
                                   });

            // If the property name not find in index's field
            // we need break this loop then check next index.
            if (it == fields.end()) {
                indexHint = false;
                break;
            }
        }
        if (!indexHint) {
            continue;
        }
        indexes.emplace_back(index->get_index_id());
    }
    return indexes;
}
void LookupExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
    auto outputs = std::make_unique<InterimResult>(std::move(returnCols_));
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

void LookupExecutor::setOutputYields(SchemaWriter *outputSchema,
                                     const std::shared_ptr<ResultSchemaProvider>& schema) {
    if (schema == nullptr) {
        return;
    }
    for (auto& col : returnCols_) {
        auto index = schema->getFieldIndex(col);
        outputSchema->appendCol(col, schema->getFieldType(index).type);
    }
}

void LookupExecutor::processEdgeResult(EdgeRpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto &resp : all) {
        if (!resp.__isset.rows || resp.get_rows() == nullptr || resp.rows.empty()) {
            continue;
        }
        if (outputSchema == nullptr) {
            if (!yields_.empty()) {
                schema = std::make_shared<ResultSchemaProvider>(*(resp.get_schema()));
            }
            outputSchema = std::make_shared<SchemaWriter>();
            outputSchema->appendCol("edgeSrc", nebula::cpp2::SupportedType::VID);
            outputSchema->appendCol("edgeDst", nebula::cpp2::SupportedType::VID);
            outputSchema->appendCol("edgeRank", nebula::cpp2::SupportedType::INT);
            setOutputYields(outputSchema.get(), schema);
            rsWriter = std::make_unique<RowSetWriter>(outputSchema);
        }
        for (const auto& data : *resp.get_rows()) {
            RowWriter writer(outputSchema);
            const auto& edge = data.get_key();
            writer << edge.get_src() << edge.get_dst() << edge.get_ranking();
            for (auto& column : returnCols_) {
                auto reader = RowReader::getRowReader(data.get_props(), schema);
                auto res = RowReader::getPropByName(reader.get(), column);
                if (!ok(res)) {
                    LOG(ERROR) << "Skip the bad value for prop " << column;
                    continue;
                }
                auto&& v = value(std::move(res));
                auto status = Collector::collect(v, &writer);
                if (!status.ok()) {
                    LOG(ERROR) << "Collect prop error: " << status;
                }
            }
            rsWriter->addRow(writer.encode());
        }
    }  // for `resp'
    finishExecution(std::move(rsWriter));
}

void LookupExecutor::processVertexResult(VertexRpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto &resp : all) {
        if (!resp.__isset.rows || resp.get_rows() == nullptr || resp.rows.empty()) {
            continue;
        }
        if (outputSchema == nullptr) {
            if (!yields_.empty()) {
                schema = std::make_shared<ResultSchemaProvider>(*(resp.get_schema()));
            }
            outputSchema = std::make_shared<SchemaWriter>();
            outputSchema->appendCol("VertexID", nebula::cpp2::SupportedType::VID);
            setOutputYields(outputSchema.get(), schema);
            rsWriter = std::make_unique<RowSetWriter>(outputSchema);
        }
        for (const auto& data : *resp.get_rows()) {
            RowWriter writer(outputSchema);
            writer << data.get_vertex_id();
            for (auto& column : returnCols_) {
                auto reader = RowReader::getRowReader(data.get_props(), schema);
                auto res = RowReader::getPropByName(reader.get(), column);
                if (!ok(res)) {
                    LOG(ERROR) << "Skip the bad value for prop " << column;
                    continue;
                }
                auto&& v = value(std::move(res));
                auto status = Collector::collect(v, &writer);
                if (!status.ok()) {
                    LOG(ERROR) << "Collect prop error: " << status;
                }
            }
            rsWriter->addRow(writer.encode());
        }
    }  // for `resp'
    finishExecution(std::move(rsWriter));
}

void LookupExecutor::stepEdgeOut() {
    auto *sc = ectx()->getStorageClient();
    auto future  = sc->lookUpEdgeIndex(spaceId_,
                                       index_,
                                       whereWrapper_->getFilterPushdown(),
                                       returnCols_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Lookup edges failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Lookup partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        processEdgeResult(std::forward<decltype(result)>(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when handle lookup: " << e.what();
        doError(Status::Error("Exception when handle lookup: %s.",
                              e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void LookupExecutor::stepVertexOut() {
    auto *sc = ectx()->getStorageClient();
    auto future  = sc->lookUpVertexIndex(spaceId_,
                                         index_,
                                         whereWrapper_->getFilterPushdown(),
                                         returnCols_);
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
        }
        processVertexResult(std::forward<decltype(result)>(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when handle lookup: " << e.what();
        doError(Status::Error("Exception when handle lookup: %s.",
                              e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void LookupExecutor::execute() {
    FLOG_INFO("Executing LOOKUP: %s", sentence_->toString().c_str());
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

    if (isEdge_) {
        stepEdgeOut();
    } else {
        stepVertexOut();
    }
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

}  // namespace graph
}  // namespace nebula
