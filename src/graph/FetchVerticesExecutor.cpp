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
        :TraverseExecutor(ectx) {
    sentence_ = static_cast<FetchVerticesSentence*>(sentence);
}

Status FetchVerticesExecutor::prepare() {
    DCHECK_NOTNULL(sentence_);
    Status status = Status::OK();
    expCtx_ = std::make_unique<ExpressionContext>();
    spaceId_ = ectx()->rctx()->session()->space();

    do {
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
    Status status = Status::OK();
    do {
        if (sentence_->isRef()) {
            auto *expr = sentence_->ref();
            if (expr->isInputExpression()) {
                auto *iexpr = static_cast<InputPropertyExpression*>(expr);
                colname_ = iexpr->prop();
            } else if (expr->isVariableExpression()) {
                auto *vexpr = static_cast<VariablePropertyExpression*>(expr);
                varname_ = vexpr->var();
                colname_ = vexpr->prop();
            } else {
                LOG(FATAL) << "Unknown kind of expression.";
            }
            break;
        }

        auto vidList = sentence_->vidList();
        for (auto *expr : vidList) {
            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval();
            if (!Expression::isInt(value)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            vids_.push_back(Expression::asInt(value));
        }
    } while (false);

    return status;
}

Status FetchVerticesExecutor::prepareYield() {
    Status status = Status::OK();
    auto *clause = sentence_->yieldClause();

    do {
        if (clause == nullptr) {
            status = setupColumns();
            if (!status.ok()) {
                break;
            }
        } else {
            yields_ = clause->columns();
        }

        for (auto *col : yields_) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
        }

        if (expCtx_->hasSrcTagProp() || expCtx_->hasDstTagProp()) {
            status = Status::SyntaxError(
                    "Only support form of alias.prop in fetch sentence.");
            break;
        }

        auto *tag = sentence_->tag();
        auto aliasProps = expCtx_->aliasProps();
        for (auto pair : aliasProps) {
            if (pair.first != *tag) {
                status = Status::SyntaxError(
                    "[%s.%s] tag not declared in %s.", pair.first, pair.second, *tag);
                break;
            }
        }
    } while (false);

    return status;
}

Status FetchVerticesExecutor::setupColumns() {
    auto *tag = sentence_->tag();
    auto result = ectx()->schemaManager()->toTagID(spaceId_, *tag);
    if (!result.ok()) {
        return result.status();
    }
    auto tagID = result.value();
    auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tagID);
    auto iter = schema->begin();
    while (iter) {
        auto *name = iter->getName();
        auto *ref = new std::string("");
        Expression *expr = new AliasPropertyExpression(ref, tag, new std::string(name));
        YieldColumn *column = new YieldColumn(expr);
        yields_.emplace_back(column);
        ++iter;
    }
    return Status::OK();
}

void FetchVerticesExecutor::execute() {
    FLOG_INFO("Executing FetchVertices: %s", sentence_->toString().c_str());
    auto status = setupVids();
    if (!status.ok()) {
        onError_(std::move(status));
        return;
    }
    if (vids_.empty()) {
        onEmptyInputs();
    }

    fetchVertices();
}

void FetchVerticesExecutor::fetchVertices() {
    auto status = getPropNames();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(status.status());
        return;
    }

    auto props = status.value();
    auto future = ectx()->storage()->getVertexProps(spaceId_, vids_, std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            DCHECK(onError_);
            onError_(Status::Error("Get props failed"));
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
        onError_(Status::Error("Internal error"));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

StatusOr<std::vector<storage::cpp2::PropDef>> FetchVerticesExecutor::getPropNames() {
    std::vector<storage::cpp2::PropDef> props;
    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::SOURCE;
        pd.name = prop.second;
        auto status = ectx()->schemaManager()->toTagID(spaceId_, prop.first);
        if (!status.ok()) {
            return Status::Error("No Schema found for '%s'", prop.first);
        }
        pd.tag_id = status.value();
        props.emplace_back(pd);
    }

    return props;
}

void FetchVerticesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    for (auto &resp : all) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        std::shared_ptr<ResultSchemaProvider> vschema;
        if (resp.get_vertex_schema() != nullptr) {
            vschema = std::make_shared<ResultSchemaProvider>(resp.vertex_schema);
        }
        for (auto &vdata : resp.vertices) {
            std::unique_ptr<RowReader> vreader;
            if (vschema != nullptr) {
                if (!vdata.__isset.vertex_data || vdata.vertex_data.empty()) {
                    continue;
                }
                vreader = RowReader::getRowReader(vdata.vertex_data, vschema);
            }
            if (outputSchema == nullptr) {
                outputSchema = std::make_shared<SchemaWriter>();
                getOutputSchema(vschema.get(), vreader.get(), outputSchema.get());
                rsWriter = std::make_unique<RowSetWriter>(outputSchema);
            }

            auto collector = std::make_unique<Collector>(vschema.get());
            auto writer = std::make_unique<RowWriter>(outputSchema);

            auto &getters = expCtx_->getters();
            getters.getAliasProp = [&] (const std::string&, const std::string &prop) {
                return collector->collect(prop, vreader.get(), writer.get());
            };
            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval();
            }

            rsWriter->addRow(*writer);
        }  // for `vdata'
    }  // for `resp'

    finishExecution(std::move(rsWriter));
}

void FetchVerticesExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
    std::unique_ptr<InterimResult> outputs;
    if (rsWriter != nullptr) {
        outputs = std::make_unique<InterimResult>(std::move(rsWriter));
    }

    if (onResult_) {
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        auto colnames = getResultColumnNames();
        resp_->set_column_names(std::move(colnames));
        if (outputs != nullptr) {
            auto rows = outputs->getRows();
            resp_->set_rows(std::move(rows));
        }
    }
    DCHECK(onFinish_);
    onFinish_();
}

void FetchVerticesExecutor::getOutputSchema(
        meta::SchemaProviderIf *schema,
        RowReader *reader,
        SchemaWriter *outputSchema) const {
    auto collector = std::make_unique<Collector>(schema);
    auto &getters = expCtx_->getters();
    getters.getAliasProp = [&] (const std::string&, const std::string &prop) {
        return collector->getProp(prop, reader);
    };
    std::vector<VariantType> record;
    for (auto *column : yields_) {
        auto *expr = column->expr();
        auto value = expr->eval();
        record.emplace_back(std::move(value));
    }

    using nebula::cpp2::SupportedType;
    auto colnames = getResultColumnNames();
    for (auto index = 0u; index < record.size(); ++index) {
        SupportedType type;
        switch (record[index].which()) {
            case 0:
                // all integers in InterimResult are regarded as type of VID
                type = SupportedType::VID;
                break;
            case 1:
                type = SupportedType::DOUBLE;
                break;
            case 2:
                type = SupportedType::BOOL;
                break;
            case 3:
                type = SupportedType::STRING;
                break;
            default:
                LOG(FATAL) << "Unknown VariantType: " << record[index].which();
        }
        outputSchema->appendCol(colnames[index], type);
    }
}

std::vector<std::string> FetchVerticesExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yields_.size());
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->expr()->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}

void FetchVerticesExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}

Status FetchVerticesExecutor::setupVids() {
    if (!vids_.empty()) {
        return Status::OK();
    }

    const auto *inputs = inputs_.get();
    // Take one column from a variable
    if (varname_ != nullptr) {
        auto *varInputs = ectx()->variableHolder()->get(*varname_);
        if (varInputs == nullptr) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
        DCHECK(inputs == nullptr);
        inputs = varInputs;
    }
    // No error happened, but we are having empty inputs
    if (inputs == nullptr) {
        return Status::OK();
    }

    auto result = inputs->getVIDs(*colname_);
    if (!result.ok()) {
        return std::move(result).status();
    }
    vids_ = std::move(result).value();
    return Status::OK();
}

void FetchVerticesExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    inputs_ = std::move(result);
}

void FetchVerticesExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}
}  // namespace graph
}  // namespace nebula
