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
            if (col->alias() == nullptr) {
                resultColNames_.emplace_back(col->expr()->toString());
            } else {
                resultColNames_.emplace_back(*col->alias());
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
                    "[%s.%s] tag not declared in %s.",
                    pair.first.c_str(), pair.second.c_str(), (*tag).c_str());
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
    if (yieldColumns_ == nullptr) {
        yieldColumns_ = std::make_unique<YieldColumns>();
    }
    while (iter) {
        auto *name = iter->getName();
        auto *ref = new std::string("");
        auto *tagName = new std::string(*tag);
        Expression *expr = new AliasPropertyExpression(ref, tagName, new std::string(name));
        YieldColumn *column = new YieldColumn(expr);
        yieldColumns_->addColumn(column);
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

    return status;
}

Status FetchVerticesExecutor::setupVidsFromRef() {
    const InterimResult *inputs;
    if (varname_ == nullptr) {
        inputs = inputs_.get();
        if (inputs == nullptr) {
            return Status::OK();
        }
    } else {
        inputs = ectx()->variableHolder()->get(*varname_);
        if (inputs == nullptr) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
    }

    auto result = inputs->getVIDs(*colname_);
    if (!result.ok()) {
        return std::move(result).status();
    }
    vids_ = std::move(result).value();
    return Status::OK();
}


}  // namespace graph
}  // namespace nebula
