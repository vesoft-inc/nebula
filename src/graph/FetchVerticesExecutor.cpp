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
        : FetchExecutor(ectx, "fetch_vertices") {
    sentence_ = static_cast<FetchVerticesSentence*>(sentence);
}

Status FetchVerticesExecutor::prepare() {
    return Status::OK();
}

Status FetchVerticesExecutor::prepareClauses() {
    Status status = Status::OK();

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        expCtx_ = std::make_unique<ExpressionContext>();
        spaceId_ = ectx()->rctx()->session()->space();
        expCtx_->setStorageClient(ectx()->getStorageClient());
        if (sentence_->isAllTagProps()) {
            break;
        }

        yieldClause_ = DCHECK_NOTNULL(sentence_)->yieldClause();
        labelName_ = sentence_->tag();
        auto result = ectx()->schemaManager()->toTagID(spaceId_, *labelName_);
        if (!result.ok()) {
            LOG(ERROR) << "Get Tag Id failed: " << result.status();
            status = result.status();
            break;
        }
        tagId_ = result.value();
        labelSchema_ = ectx()->schemaManager()->getTagSchema(spaceId_, tagId_);
        if (labelSchema_ == nullptr) {
            LOG(ERROR) << *labelName_ << " tag schema not exist.";
            status = Status::Error("%s tag schema not exist.", labelName_->c_str());
            break;
        }

        status = prepareVids();
        if (!status.ok()) {
            LOG(ERROR) << "Prepare vertex id failed: " << status;
            break;
        }

        // Add VertexID before prepareYield()
        returnColNames_.emplace_back("VertexID");
        status = prepareYield();
        if (!status.ok()) {
            LOG(ERROR) << "Prepare yield failed: " << status;
            break;
        }
        status = checkTagProps();
        if (!status.ok()) {
            LOG(ERROR) << "Check props failed: " << status;
            break;
        }
    } while (false);
    return status;
}

Status FetchVerticesExecutor::prepareVids() {
    if (sentence_->isRef()) {
        auto *expr = sentence_->ref();
        if (expr->isInputExpression()) {
            auto *iexpr = static_cast<InputPropertyExpression*>(expr);
            colname_ = iexpr->prop();
        } else if (expr->isVariableExpression()) {
            auto *vexpr = static_cast<VariablePropertyExpression*>(expr);
            varname_ = vexpr->alias();
            colname_ = vexpr->prop();
        } else {
            //  should never come to here.
            //  only support input and variable yet.
            LOG(ERROR) << "Unknown kind of expression.";
            return Status::Error("Unknown kind of expression.");
        }
        if (colname_ != nullptr && *colname_ == "*") {
            return Status::Error("Cant not use `*' to reference a vertex id column.");
        }
    }
    return Status::OK();
}

Status FetchVerticesExecutor::checkTagProps() {
    auto aliasProps = expCtx_->aliasProps();
    for (auto &pair : aliasProps) {
        if (pair.first != *labelName_) {
            return Status::SyntaxError(
                "Near [%s.%s], tag should be declared in `ON' clause first.",
                    pair.first.c_str(), pair.second.c_str());
        }

        if (labelSchema_->getFieldIndex(pair.second) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                    pair.second.c_str(), pair.first.c_str());
        }
    }
    return Status::OK();
}

void FetchVerticesExecutor::execute() {
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    status = setupVids();
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
    std::vector<storage::cpp2::PropDef> props;
    if (!sentence_->isAllTagProps()) {
        props = getPropNames();
    }

    auto future = ectx()->getStorageClient()->getVertexProps(spaceId_, vids_, std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get tag `%s' props failed", sentence_->tag()->c_str()));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get vertices partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        if (!sentence_->isAllTagProps()) {
            processResult(std::move(result));
        } else {
            processAllPropsResult(std::move(result));
        }
        return;
    };
    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Get tag `%s' props exception: %s.",
                sentence_->tag()->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

std::vector<storage::cpp2::PropDef> FetchVerticesExecutor::getPropNames() {
    std::vector<storage::cpp2::PropDef> props;
    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::SOURCE;
        pd.name = prop.second;
        pd.id.set_tag_id(tagId_);
        props.emplace_back(std::move(pd));
    }

    return props;
}

void FetchVerticesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    Getters getters;
    for (auto &resp : all) {
        if (!resp.__isset.vertices) {
            continue;
        }

        if (resultColNames_.empty()) {
            for (auto &vdata : resp.vertices) {
                if (outputSchema == nullptr) {
                    outputSchema = std::make_shared<SchemaWriter>();
                    outputSchema->appendCol("VertexID", nebula::cpp2::SupportedType::VID);
                    rsWriter = std::make_unique<RowSetWriter>(outputSchema);
                }
                auto writer = std::make_unique<RowWriter>(outputSchema);
                (*writer) << vdata.vertex_id;
                std::string encode = writer->encode();
                rsWriter->addRow(std::move(encode));
            }
        } else {
            auto *schema = resp.get_vertex_schema();
            if (schema == nullptr) {
                continue;
            }

            std::unordered_map<TagID, std::shared_ptr<ResultSchemaProvider>> tagSchema;
            std::transform(schema->cbegin(), schema->cend(),
                        std::inserter(tagSchema, tagSchema.begin()), [](auto &s) {
                            return std::make_pair(
                                s.first, std::make_shared<ResultSchemaProvider>(s.second));
                        });

            for (auto &vdata : resp.vertices) {
                std::unique_ptr<RowReader> vreader;
                if (!vdata.__isset.tag_data || vdata.tag_data.empty()) {
                    continue;
                }

                auto vschema = tagSchema[vdata.tag_data[0].tag_id];
                vreader = RowReader::getRowReader(vdata.tag_data[0].data, vschema);
                if (outputSchema == nullptr) {
                    outputSchema = std::make_shared<SchemaWriter>();
                    outputSchema->appendCol("VertexID", nebula::cpp2::SupportedType::VID);
                    auto status = getOutputSchema(vschema.get(), vreader.get(), outputSchema.get());
                    if (!status.ok()) {
                        LOG(ERROR) << "Get output schema failed: " << status;
                        doError(Status::Error("Get output schema failed: %s.",
                                    status.toString().c_str()));
                        return;
                    }
                    rsWriter = std::make_unique<RowSetWriter>(outputSchema);
                }

                auto writer = std::make_unique<RowWriter>(outputSchema);
                (*writer) << vdata.vertex_id;
                getters.getAliasProp =
                    [&vreader, &vschema] (const std::string&,
                                        const std::string &prop) -> OptVariantType {
                    return Collector::getProp(vschema.get(), prop, vreader.get());
                };
                for (auto *column : yields_) {
                    auto *expr = column->expr();
                    auto value = expr->eval(getters);
                    if (!value.ok()) {
                        doError(std::move(value).status());
                        return;
                    }
                    auto status = Collector::collect(value.value(), writer.get());
                    if (!status.ok()) {
                        LOG(ERROR) << "Collect prop error: " << status;
                        doError(std::move(status));
                        return;
                    }
                }
                // TODO Consider float/double, and need to reduce mem copy.
                std::string encode = writer->encode();
                rsWriter->addRow(std::move(encode));
            }  // for `vdata'
        }
    }  // for `resp'

    finishExecution(std::move(rsWriter));
}

Status FetchVerticesExecutor::setupVids() {
    Status status = Status::OK();
    if (sentence_->isRef() && !sentence_->isAllTagProps()) {
        status = setupVidsFromRef();
    } else {
        status = setupVidsFromExpr();
    }

    return status;
}

Status FetchVerticesExecutor::setupVidsFromExpr() {
    Status status = Status::OK();
    std::unique_ptr<std::unordered_set<VertexID>> uniqID;
    if (distinct_) {
        uniqID = std::make_unique<std::unordered_set<VertexID>>();
    }

    expCtx_->setSpace(spaceId_);
    auto vidList = sentence_->vidList();
    Getters getters;
    for (auto *expr : vidList) {
        expr->setContext(expCtx_.get());
        status = expr->prepare();
        if (!status.ok()) {
            break;
        }
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
            auto result = uniqID->emplace(valInt);
            if (result.second) {
                vids_.emplace_back(valInt);
            }
        } else {
            vids_.emplace_back(valInt);
        }
    }

    return status;
}

Status FetchVerticesExecutor::setupVidsFromRef() {
    const InterimResult *inputs;
    if (varname_ == nullptr) {
        inputs = inputs_.get();
    } else {
        bool existing = false;
        inputs = ectx()->variableHolder()->get(*varname_, &existing);
        if (!existing) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
    }
    if (inputs == nullptr || !inputs->hasData()) {
        return Status::OK();
    }

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        return status;
    }
    StatusOr<std::vector<VertexID>> result;
    if (distinct_) {
        result = inputs->getDistinctVIDs(*colname_);
    } else {
        result = inputs->getVIDs(*colname_);
    }
    if (!result.ok()) {
        return std::move(result).status();
    }
    vids_ = std::move(result).value();
    return Status::OK();
}

void FetchVerticesExecutor::processAllPropsResult(RpcResponse &&result) {
    auto &all = result.responses();
    std::unique_ptr<RowSetWriter> rsWriter;
    std::shared_ptr<SchemaWriter> outputSchema;
    for (auto &resp : all) {
        if (!resp.__isset.vertices) {
            continue;
        }

        for (auto &vdata : resp.vertices) {
            if (!vdata.__isset.tag_data || vdata.tag_data.empty()) {
                continue;
            }
            RowWriter writer;
            writer << RowWriter::ColType(nebula::cpp2::SupportedType::VID) << vdata.vertex_id;
            for (auto &tdata : vdata.tag_data) {
                auto ver = RowReader::getSchemaVer(tdata.data);
                if (ver < 0) {
                    LOG(ERROR) << "Found schema version negative " << ver;
                    doError(Status::Error("Found schema version negative: %d", ver));
                    return;
                }
                auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tdata.tag_id, ver);
                if (schema == nullptr) {
                    VLOG(3) << "Schema not found for tag id: " << tdata.tag_id;
                    // Ignore the bad data.
                    continue;
                }
                if (rsWriter == nullptr) {
                    outputSchema = std::make_shared<SchemaWriter>();
                    outputSchema->appendCol("VertexID", nebula::cpp2::SupportedType::VID);
                    returnColNames_.emplace_back("VertexID");
                    rsWriter = std::make_unique<RowSetWriter>(outputSchema);
                }
                // row.append(tdata.data);
                auto reader = RowReader::getRowReader(tdata.data, schema);

                auto tagFound = ectx()->schemaManager()->toTagName(spaceId_, tdata.tag_id);
                if (!tagFound.ok()) {
                    VLOG(3) << "Tag name not found for tag id: " << tdata.tag_id;
                    // Ignore the bad data.
                    continue;
                }
                auto tagName = std::move(tagFound).value();
                auto iter = schema->begin();
                auto index = 0;

                while (iter) {
                    auto *field = iter->getName();
                    auto prop = RowReader::getPropByIndex(reader.get(), index);
                    if (!ok(prop)) {
                        LOG(ERROR) << "Read props of tag " << tagName << " failed.";
                        doError(Status::Error("Read props of tag `%s' failed.", tagName.c_str()));
                        return;
                    }
                    Collector::collectWithoutSchema(value(prop), &writer);
                    auto colName = folly::stringPrintf("%s.%s", tagName.c_str(), field);
                    resultColNames_.emplace_back(colName);
                    returnColNames_.emplace_back(colName);
                    auto fieldType = iter->getType();
                    outputSchema->appendCol(std::move(colName), std::move(fieldType));
                    ++index;
                    ++iter;
                }
            }
            if (writer.size() > 1 && rsWriter != nullptr) {
                rsWriter->addRow(writer.encode());
            }
        }
    }

    finishExecution(std::move(rsWriter));
}
}  // namespace graph
}  // namespace nebula
