/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/FetchVerticesExecutor.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/SchemaWriter.h"

namespace nebula {
namespace graph {

FetchVerticesExecutor::FetchVerticesExecutor(Sentence *sentence, ExecutionContext *ectx)
        : TraverseExecutor(ectx, "fetch_vertices") {
    sentence_ = dynamic_cast<FetchVerticesSentence*>(sentence);
}

Status FetchVerticesExecutor::prepare() {
    return Status::OK();
}

Status FetchVerticesExecutor::prepareVids() {
    Status status = Status::OK();
    if (sentence_->isRef()) {
        fromType_ = kRef;
        auto *expr = sentence_->ref();
        if (expr->isInputExpression()) {
            auto *iexpr = dynamic_cast<InputPropertyExpression*>(expr);
            colname_ = iexpr->prop();
            inputsPtr_ = inputs_.get();
        } else if (expr->isVariableExpression()) {
            auto *vexpr = dynamic_cast<VariablePropertyExpression*>(expr);
            auto varname = vexpr->alias();
            colname_ = vexpr->prop();
            bool existing = false;
            inputsPtr_ = ectx()->variableHolder()->get(*varname, &existing);
            if (!existing) {
                return Status::Error("Variable `%s' not defined", varname->c_str());
            }
        } else {
            //  should never come to here.
            //  only support input and variable yet.
            LOG(ERROR) << "Unknown kind of expression.";
            return Status::Error("Unknown kind of expression.");
        }
        if (colname_ != nullptr && *colname_ == "*") {
            return Status::Error("Cant not use `*' to reference a vertex id column.");
        }
        if (inputsPtr_ == nullptr || !inputsPtr_->hasData()) {
            return Status::OK();
        }
        status = checkIfDuplicateColumn();
        if (!status.ok()) {
            return status;
        }
        auto vidsStatus = inputsPtr_->getDistinctVIDs(*colname_);
        if (!vidsStatus.ok()) {
            return std::move(vidsStatus).status();
        }
        vids_ = std::move(vidsStatus).value();
        return Status::OK();
    } else {
        fromType_ = kInstantExpr;
        std::unordered_set<VertexID> uniqID;
        for (auto *expr : sentence_->vidList()) {
            expr->setContext(expCtx_.get());
            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            Getters getters;
            auto value = expr->eval(getters);
            if (!value.ok()) {
                return value.status();
            }
            auto v = value.value();
            if (!Expression::isInt(v)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            auto valInt = Expression::asInt(v);
            if (distinct_) {
                auto result = uniqID.emplace(valInt);
                if (result.second) {
                    vids_.emplace_back(valInt);
                }
            } else {
                vids_.emplace_back(valInt);
            }
        }
    }
    return status;
}

Status FetchVerticesExecutor::prepareTags() {
    Status status = Status::OK();
    auto* tags = sentence_->tags();
    if (tags == nullptr) {
        LOG(ERROR) << "tags shall never be null";
        return Status::Error("tags shall never be null");
    }

    auto tagNames = tags->labels();
    if (tagNames.empty()) {
        LOG(ERROR) << "tags shall never be empty";
        return Status::Error("tags shall never be empty");
    }

    if (tagNames.size() == 1 && *tagNames[0] == "*") {
        auto tagsStatus = ectx()->schemaManager()->getAllTag(spaceId_);
        if (!tagsStatus.ok()) {
            return tagsStatus.status();
        }
        for (auto& tagName : std::move(tagsStatus).value()) {
            auto result = tagNameSet_.emplace(tagName);
            if (!result.second) {
                return Status::Error(folly::sformat("tag({}) was dup", tagName));
            }
        }
    } else {
        for (auto tagName : tagNames) {
            auto tagStatus = ectx()->schemaManager()->toTagID(spaceId_, *tagName);
            if (!tagStatus.ok()) {
                return tagStatus.status();
            }
            auto tagId = tagStatus.value();
            tagNames_.push_back(*tagName);
            tagIds_.push_back(tagId);
            auto result = tagNameSet_.emplace(*tagName);
            if (!result.second) {
                return Status::Error(folly::sformat("tag({}) was dup", *tagName));
            }
        }
    }

    return status;
}

Status FetchVerticesExecutor::prepareYield() {
    colNames_.emplace_back("VertexID");
    colTypes_.emplace_back(nebula::cpp2::SupportedType::VID);
    if (yieldClause_ == nullptr) {
        // determine which columns to return after received response from storage.
        for (unsigned i = 0; i < tagNames_.size(); i++) {
            auto& tagName = tagNames_[i];
            auto tagId = tagIds_[i];
            std::shared_ptr<const meta::SchemaProviderIf> tagSchema =
                ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
            if (tagSchema == nullptr) {
                return Status::Error("No tag schema for %s", tagName.c_str());
            }
            for (auto iter = tagSchema->begin(); iter != tagSchema->end(); ++iter) {
                auto *prop = iter->getName();
                storage::cpp2::PropDef pd;
                pd.owner = storage::cpp2::PropOwner::SOURCE;
                pd.name = prop;
                pd.id.set_tag_id(tagId);
                props_.emplace_back(std::move(pd));
            }
        }
    } else {
        for (auto *col : yieldClause_->columns()) {
            if (!col->getFunName().empty()) {
                return Status::SyntaxError("Do not support aggregated query with fetch prop on.");
            }

            if (col->expr()->isInputExpression()) {
                auto *inputExpr = dynamic_cast<InputPropertyExpression*>(col->expr());
                auto *colName = inputExpr->prop();
                if (*colName == "*") {
                    auto colNames = inputsPtr_->getColNames();
                    for (auto &prop : colNames) {
                        Expression *expr = new InputPropertyExpression(new std::string(prop));
                        auto *column = new YieldColumn(expr);
                        yieldColsHolder_.addColumn(column);
                        yields_.emplace_back(column);
                        colNames_.emplace_back(column->toString());
                        colTypes_.emplace_back(nebula::cpp2::SupportedType::UNKNOWN);
                        expCtx_->addInputProp(prop);
                    }
                    continue;
                }
            } else if (col->expr()->isVariableExpression()) {
                auto *variableExpr = dynamic_cast<VariablePropertyExpression*>(col->expr());
                auto *colName = variableExpr->prop();
                if (*colName == "*") {
                    auto colNames = inputsPtr_->getColNames();
                    for (auto &prop : colNames) {
                        auto *alias = new std::string(*(variableExpr->alias()));
                        Expression *expr =
                            new VariablePropertyExpression(alias, new std::string(prop));
                        auto *column = new YieldColumn(expr);
                        yieldColsHolder_.addColumn(column);
                        yields_.emplace_back(column);
                        colNames_.emplace_back(column->toString());
                        colTypes_.emplace_back(nebula::cpp2::SupportedType::UNKNOWN);
                        expCtx_->addInputProp(prop);
                    }
                    continue;
                }
            }

            yields_.emplace_back(col);
            col->expr()->setContext(expCtx_.get());
            Status status = col->expr()->prepare();
            if (!status.ok()) {
                return status;
            }
            if (col->alias() == nullptr) {
                colNames_.emplace_back(col->expr()->toString());
            } else {
                colNames_.emplace_back(*col->alias());
            }
            auto type = calculateExprType(col->expr());
            colTypes_.emplace_back(type);
            VLOG(1) << "type: " << static_cast<int64_t>(colTypes_.back());
        }
        if (expCtx_->hasSrcTagProp() || expCtx_->hasDstTagProp()) {
            return Status::SyntaxError(
                "tag.prop and edgetype.prop are supported in fetch sentence.");
        }

        auto aliasProps = expCtx_->aliasProps();
        for (auto &pair : aliasProps) {
            auto& tagName = pair.first;
            auto& prop = pair.second;
            if (tagNameSet_.find(tagName) == tagNameSet_.end()) {
                return Status::SyntaxError(
                    "Near [%s.%s], tag should be declared in `ON' clause first.",
                    tagName.c_str(), prop.c_str());
            }
            auto tagStatus = ectx()->schemaManager()->toTagID(spaceId_, tagName);
            if (!tagStatus.ok()) {
                return tagStatus.status();
            }
            auto tagId = tagStatus.value();

            std::shared_ptr<const meta::SchemaProviderIf> tagSchema =
                ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
            if (tagSchema == nullptr) {
                return Status::Error("No tag schema for %s", tagName.c_str());
            }
            if (tagSchema->getFieldIndex(prop) == -1) {
                return Status::Error(
                    "`%s' is not a prop of `%s'", tagName.c_str(), prop.c_str());
            }
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::SOURCE;
            pd.name = prop;
            pd.id.set_tag_id(tagId);
            props_.emplace_back(std::move(pd));
        }
    }
    return Status::OK();
}

Status FetchVerticesExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    spaceId_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);

    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        yieldClause_ = sentence_->yieldClause();
        if (yieldClause_ != nullptr) {
            distinct_ = yieldClause_->isDistinct();
        }
        status = prepareVids();
        if (!status.ok()) {
            break;
        }
        status = prepareTags();
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

void FetchVerticesExecutor::onEmptyInputs() {
    if (onResult_) {
        auto outputs = std::make_unique<InterimResult>(std::move(colNames_));
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
    }
    doFinish(Executor::ProcessControl::kNext);
}

void FetchVerticesExecutor::execute() {
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    if (vids_.empty()) {
        LOG(WARNING) << "Empty vids";
        onEmptyInputs();
        return;
    }
    fetchVertices();
}

void FetchVerticesExecutor::fetchVertices() {
    auto future = ectx()->getStorageClient()->getVertexProps(
        spaceId_, vids_, std::move(props_));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get tag props failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get vertices partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            ectx()->addWarningMsg("Fetch vertices executor was partially performed");
        }
        processResult(std::move(result));
    };
    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Get tag props exception: %s.", e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void FetchVerticesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    size_t num = 0;
    for (auto &resp : all) {
        num += resp.vertices.size();
    }
    if (num == 0) {
        finishExecution(std::move(rsWriter));
        return;
    }
    std::unordered_map<VertexID, std::map<TagID, RowReader>> dataMap;
    dataMap.reserve(num);

    std::unordered_map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> tagSchemaMap;
    std::set<TagID> tagIdSet;
    for (auto &resp : all) {
        if (!resp.__isset.vertices || resp.vertices.empty()) {
            continue;
        }
        auto *vertexSchema = resp.get_vertex_schema();
        if (vertexSchema != nullptr) {
            std::transform(vertexSchema->cbegin(), vertexSchema->cend(),
                           std::inserter(tagSchemaMap, tagSchemaMap.begin()), [](auto &s) {
                    return std::make_pair(
                        s.first, std::make_shared<ResultSchemaProvider>(s.second));
                });
        }

        for (auto &vdata : resp.vertices) {
            if (!vdata.__isset.tag_data || vdata.tag_data.empty()) {
                continue;
            }
            for (auto& tagData : vdata.tag_data) {
                auto& data = tagData.data;
                VertexID vid = vdata.vertex_id;
                TagID tagId = tagData.tag_id;
                if (tagSchemaMap.find(tagId) == tagSchemaMap.end()) {
                    auto ver = RowReader::getSchemaVer(data);
                    if (ver < 0) {
                        LOG(ERROR) << "Found schema version negative " << ver;
                        doError(Status::Error("Found schema version negative: %d", ver));
                        return;
                    }
                    auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tagId, ver);
                    if (schema == nullptr) {
                        VLOG(3) << "Schema not found for tag id: " << tagId;
                        // Ignore the bad data.
                        continue;
                    }
                    tagSchemaMap[tagId] = schema;
                }
                auto vschema = tagSchemaMap[tagId];
                auto vreader = RowReader::getRowReader(data, vschema);
                dataMap[vid].emplace(std::make_pair(tagId, std::move(vreader)));
                tagIdSet.insert(tagId);
            }
        }
    }

    if (yieldClause_ == nullptr) {
        for (TagID tagId : tagIdSet) {
            auto tagSchema = tagSchemaMap[tagId];
            auto tagFound = ectx()->schemaManager()->toTagName(spaceId_, tagId);
            if (!tagFound.ok()) {
                VLOG(3) << "Tag name not found for tag id: " << tagId;
                // Ignore the bad data.
                continue;
            }
            auto tagName = std::move(tagFound).value();

            for (auto iter = tagSchema->begin(); iter != tagSchema->end(); ++iter) {
                auto *ref = new std::string("");
                auto *alias = new std::string(tagName);
                auto *prop = iter->getName();
                Expression *expr =
                    new AliasPropertyExpression(ref, alias, new std::string(prop));
                auto *column = new YieldColumn(expr);
                yieldColsHolder_.addColumn(column);
                yields_.emplace_back(column);
                colNames_.emplace_back(expr->toString());
                colTypes_.emplace_back(iter->getType().type);
            }
        }
    }

    if (fromType_ == kRef) {
        if (inputsPtr_ == nullptr) {
            LOG(ERROR) << "inputs is nullptr.";
            doError(Status::Error("inputs is nullptr."));
            return;
        }

        auto visitor = [&, this] (const RowReader *reader) -> Status {
            VertexID vid = 0;
            auto rc = reader->getVid(*colname_, vid);
            if (rc != ResultType::SUCCEEDED) {
                return Status::Error("Column `%s' not found", colname_->c_str());
            }
            if (dataMap.find(vid) == dataMap.end() && !expCtx_->hasInputProp()) {
                return Status::OK();
            }
            // if yield input not empty, create empty item and keep on going.
            auto& ds = dataMap[vid];

            std::vector<VariantType> record;
            record.emplace_back(VariantType(vid));

            auto schema = reader->getSchema().get();
            Getters getters;
            getters.getVariableProp = [&] (const std::string &prop) -> OptVariantType {
                return Collector::getProp(schema, prop, reader);
            };
            getters.getInputProp = [&] (const std::string &prop) -> OptVariantType {
                return Collector::getProp(schema, prop, reader);
            };
            getters.getAliasProp = [&] (const std::string& tagName, const std::string &prop)
                -> OptVariantType {
                auto tagIdStatus = ectx()->schemaManager()->toTagID(spaceId_, tagName);
                if (!tagIdStatus.ok()) {
                    return tagIdStatus.status();
                }
                TagID tagId = std::move(tagIdStatus).value();
                auto tagIter = ds.find(tagId);
                if (tagIter != ds.end()) {
                    auto vreader = tagIter->second.get();
                    auto vschema = vreader->getSchema().get();
                    return Collector::getProp(vschema, prop, vreader);
                } else {
                    auto ts = ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
                    if (ts == nullptr) {
                        return Status::Error("No tag schema for %s", tagName.c_str());
                    }
                    return RowReader::getDefaultProp(ts.get(), prop);
                }
            };

            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval(getters);
                if (!value.ok()) {
                    return value.status();
                }
                record.emplace_back(std::move(value).value());
            }
            if (outputSchema == nullptr) {
                outputSchema = std::make_shared<SchemaWriter>();
                rsWriter = std::make_unique<RowSetWriter>(outputSchema);
                auto getSchemaStatus = Collector::getSchema(
                    record, colNames_, colTypes_, outputSchema.get());
                if (!getSchemaStatus.ok()) {
                    return getSchemaStatus;
                }
            }
            auto writer = std::make_unique<RowWriter>(outputSchema);
            for (auto& value : record) {
                auto status = Collector::collect(value, writer.get());
                if (!status.ok()) {
                    return status;
                }
            }
            rsWriter->addRow(*writer);
            return Status::OK();
        };

        Status status = inputsPtr_->applyTo(visitor);
        if (!status.ok()) {
            LOG(ERROR) << "inputs visit failed. " << status.toString();
            doError(status);
            return;
        }
    } else {
        for (auto vid : vids_) {
            auto iter = dataMap.find(vid);
            if (iter == dataMap.end()) {
                continue;
            }
            if (dataMap.find(vid) == dataMap.end()) {
                continue;
            }
            auto& ds = dataMap[vid];

            std::vector<VariantType> record;
            record.emplace_back(VariantType(vid));

            Getters getters;
            getters.getAliasProp = [&] (const std::string& tagName, const std::string &prop)
                -> OptVariantType {
                auto tagIdStatus = ectx()->schemaManager()->toTagID(spaceId_, tagName);
                if (!tagIdStatus.ok()) {
                    return tagIdStatus.status();
                }
                TagID tagId = std::move(tagIdStatus).value();
                auto tagIter = ds.find(tagId);
                if (tagIter != ds.end()) {
                    auto vreader = tagIter->second.get();
                    auto vschema = vreader->getSchema().get();
                    return Collector::getProp(vschema, prop, vreader);
                } else {
                    auto ts = ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
                    if (ts == nullptr) {
                        return Status::Error("No tag schema for %s", tagName.c_str());
                    }
                    return RowReader::getDefaultProp(ts.get(), prop);
                }
            };

            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval(getters);
                if (!value.ok()) {
                    doError(value.status());
                    return;
                }
                record.emplace_back(std::move(value).value());
            }
            if (outputSchema == nullptr) {
                outputSchema = std::make_shared<SchemaWriter>();
                rsWriter = std::make_unique<RowSetWriter>(outputSchema);
                auto getSchemaStatus = Collector::getSchema(
                    record, colNames_, colTypes_, outputSchema.get());
                if (!getSchemaStatus.ok()) {
                    doError(getSchemaStatus);
                    return;
                }
            }
            auto writer = std::make_unique<RowWriter>(outputSchema);
            for (auto& value : record) {
                auto status = Collector::collect(value, writer.get());
                if (!status.ok()) {
                    doError(status);
                    return;
                }
            }
            rsWriter->addRow(*writer);
        }
    }

    finishExecution(std::move(rsWriter));
}

void FetchVerticesExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(colNames_));
    }
    resp = std::move(*resp_);
}

void FetchVerticesExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
    auto outputs = std::make_unique<InterimResult>(std::move(colNames_));
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

}  // namespace graph
}  // namespace nebula
