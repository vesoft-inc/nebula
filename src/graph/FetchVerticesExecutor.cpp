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
        : FetchExecutor(ectx) {
    sentence_ = static_cast<FetchVerticesSentence*>(sentence);
}

Status FetchVerticesExecutor::prepare() {
    return Status::OK();
}

Status FetchVerticesExecutor::prepareClauses() {
    DCHECK_NOTNULL(sentence_);
    Status status = Status::OK();

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        expCtx_ = std::make_unique<ExpressionContext>();
        expCtx_->setStorageClient(ectx()->getStorageClient());

        spaceId_ = ectx()->rctx()->session()->space();
        yieldClause_ = sentence_->yieldClause();
        labelName_ = sentence_->tag();
        auto result = ectx()->schemaManager()->toTagID(spaceId_, *labelName_);
        if (!result.ok()) {
            status = result.status();
            break;
        }
        tagID_ = result.value();
        labelSchema_ = ectx()->schemaManager()->getTagSchema(spaceId_, tagID_);
        if (labelSchema_ == nullptr) {
            LOG(ERROR) << *labelName_ << " tag schema not exist.";
            status = Status::Error("%s tag schema not exist.", labelName_->c_str());
            break;
        }

        status = prepareVids();
        if (!status.ok()) {
            break;
        }
        status = prepareYield();
        if (!status.ok()) {
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
            LOG(FATAL) << "Unknown kind of expression.";
        }
        if (colname_ != nullptr && *colname_ == "*") {
            return Status::Error("Cant not use `*' to reference a vertex id column.");
        }
    }
    return Status::OK();
}

void FetchVerticesExecutor::execute() {
    FLOG_INFO("Executing FetchVertices: %s", sentence_->toString().c_str());
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getFetchVerticesStats());
        return;
    }

    status = setupVids();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getFetchVerticesStats());
        return;
    }
    if (vids_.empty()) {
        onEmptyInputs();
        return;
    }

    fetchVertices();
}

void FetchVerticesExecutor::fetchVertices() {
    auto props = getPropNames();
    if (props.empty()) {
        doError(Status::Error("No props declared."),
                ectx()->getGraphStats()->getFetchVerticesStats());
        return;
    }

    auto future = ectx()->getStorageClient()->getVertexProps(spaceId_, vids_, std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get props failed"),
                    ectx()->getGraphStats()->getFetchVerticesStats());
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get vertices partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        processResult(std::move(result));
        return;
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"), ectx()->getGraphStats()->getFetchVerticesStats());
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

std::vector<storage::cpp2::PropDef> FetchVerticesExecutor::getPropNames() {
    std::vector<storage::cpp2::PropDef> props;
    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::SOURCE;
        pd.name = prop.second;
        pd.id.set_tag_id(tagID_);
        props.emplace_back(std::move(pd));
    }

    return props;
}

void FetchVerticesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto uniqResult = std::make_unique<std::unordered_set<std::string>>();
    for (auto &resp : all) {
        if (!resp.__isset.vertices) {
            continue;
        }

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
                auto status = getOutputSchema(vschema.get(), vreader.get(), outputSchema.get());
                if (!status.ok()) {
                    LOG(ERROR) << "Get getOutputSchema failed: " << status;
                    doError(Status::Error("Internal error."),
                            ectx()->getGraphStats()->getFetchVerticesStats());
                    return;
                }
                rsWriter = std::make_unique<RowSetWriter>(outputSchema);
            }

            auto writer = std::make_unique<RowWriter>(outputSchema);
            auto &getters = expCtx_->getters();
            getters.getAliasProp =
                [&vreader, &vschema] (const std::string&,
                                      const std::string &prop) -> OptVariantType {
                return Collector::getProp(vschema.get(), prop, vreader.get());
            };
            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval();
                if (!value.ok()) {
                    doError(std::move(value).status(),
                            ectx()->getGraphStats()->getFetchVerticesStats());
                    return;
                }
                auto status = Collector::collect(value.value(), writer.get());
                if (!status.ok()) {
                    LOG(ERROR) << "Collect prop error: " << status;
                    doError(std::move(status), ectx()->getGraphStats()->getFetchVerticesStats());
                    return;
                }
            }
            // TODO Consider float/double, and need to reduce mem copy.
            std::string encode = writer->encode();
            if (distinct_) {
                auto ret = uniqResult->emplace(encode);
                if (ret.second) {
                    rsWriter->addRow(std::move(encode));
                }
            } else {
                rsWriter->addRow(std::move(encode));
            }
        }  // for `vdata'
    }      // for `resp'

    finishExecution(std::move(rsWriter));
}

Status FetchVerticesExecutor::setupVids() {
    Status status = Status::OK();
    if (sentence_->isRef()) {
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
    for (auto *expr : vidList) {
        expr->setContext(expCtx_.get());
        status = expr->prepare();
        if (!status.ok()) {
            break;
        }
        auto value = expr->eval();
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
}  // namespace graph
}  // namespace nebula
