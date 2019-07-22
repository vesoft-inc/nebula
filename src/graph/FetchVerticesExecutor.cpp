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

    do {
        expCtx_ = std::make_unique<ExpressionContext>();
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
            return Status::Error("%s tag schema not exist.", labelName_->c_str());
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
    Status status = Status::OK();
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
            status = Status::Error("Unkown kind of expression.");
            LOG(FATAL) << "Unknown kind of expression.";
        }
    }

    return status;
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
        pd.tag_id = tagID_;
        props.emplace_back(std::move(pd));
    }

    return props;
}

void FetchVerticesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    for (auto &resp : all) {
        if (!resp.__isset.vertices || !resp.__isset.vertex_schema
                || resp.get_vertices() == nullptr || resp.get_vertex_schema() == nullptr) {
            continue;
        }
        auto vschema = std::make_shared<ResultSchemaProvider>(resp.vertex_schema);
        for (auto &vdata : resp.vertices) {
            std::unique_ptr<RowReader> vreader;
            if (!vdata.__isset.vertex_data || vdata.vertex_data.empty()) {
                continue;
            }
            vreader = RowReader::getRowReader(vdata.vertex_data, vschema);
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
                expr->eval();
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
