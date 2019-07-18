/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/FetchEdgesExecutor.h"

namespace nebula {
namespace graph {
FetchEdgesExecutor::FetchEdgesExecutor(Sentence *sentence, ExecutionContext *ectx)
        :TraverseExecutor(ectx) {
    sentence_ = static_cast<FetchEdgesSentence*>(sentence);
}

Status FetchEdgesExecutor::prepare() {
    DCHECK_NOTNULL(sentence_);
    Status status = Status::OK();
    expCtx_ = std::make_unique<ExpressionContext>();
    spaceId_ = ectx()->rctx()->session()->space();

    do {
        status = prepareEdgeKeys();
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

Status FetchEdgesExecutor::prepareEdgeKeys() {
    Status status = Status::OK();
    do {
        auto *label = sentence_->label();
        auto result = ectx()->schemaManager()->toEdgeType(spaceId_, *label);
        if (!result.ok()) {
            status = result.status();
            break;
        }
        edgeType_ = result.value();

        if (sentence_->isRef()) {
            auto *edgeKeyRef = sentence_->ref();

            srcid_ = edgeKeyRef->srcid();
            DCHECK_NOTNULL(srcid_);

            dstid_ = edgeKeyRef->dstid();
            DCHECK_NOTNULL(dstid_);

            rank_ = edgeKeyRef->rank();
            auto ret = edgeKeyRef->varname();
            if (!ret.ok()) {
                status = ret.status();
                break;
            }
            varname_ = ret.value();
            break;
        }

        auto edgeKeyExprs = sentence_->keys()->keys();
        for (auto *keyExpr : edgeKeyExprs) {
            auto *srcExpr = keyExpr->srcid();
            auto *dstExpr = keyExpr->dstid();
            auto rank = keyExpr->rank();

            status = srcExpr->prepare();
            if (!status.ok()) {
                break;
            }
            status = dstExpr->prepare();
            if (!status.ok()) {
                break;
            }
            auto srcid = srcExpr->eval();
            auto dstid = dstExpr->eval();
            if (!Expression::isInt(srcid) || !Expression::isInt(dstid)) {
                status = Status::Error("ID should be of type integer.");
                break;
            }
            storage::cpp2::EdgeKey key;
            key.set_src(Expression::asInt(srcid));
            key.set_edge_type(edgeType_);
            key.set_dst(Expression::asInt(dstid));
            key.set_ranking(rank);

            edgeKeys_.emplace_back(std::move(key));
        }
    } while (false);

    return status;
}

Status FetchEdgesExecutor::prepareYield() {
    Status status = Status::OK();
    auto *clause = sentence_->yieldClause();

    do {
        if (clause == nullptr) {
            setupColumns();
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

        auto *label = sentence_->label();
        auto aliasProps = expCtx_->aliasProps();
        for (auto pair : aliasProps) {
            if (pair.first != *label) {
                status = Status::SyntaxError(
                    "[%s.%s] not declared in %s.",
                    pair.first.c_str(), pair.second.c_str(), (*label).c_str());
                break;
            }
        }
    } while (false);

    return status;
}

Status FetchEdgesExecutor::setupColumns() {
    auto label = sentence_->label();
    auto schema = ectx()->schemaManager()->getEdgeSchema(spaceId_, edgeType_);
    auto iter = schema->begin();
    while (iter) {
        auto *name = iter->getName();
        auto *ref = new std::string("");
        Expression *expr = new AliasPropertyExpression(ref, label, new std::string(name));
        YieldColumn *column = new YieldColumn(expr);
        yields_.emplace_back(column);
        ++iter;
    }
    return Status::OK();
}

void FetchEdgesExecutor::execute() {
    FLOG_INFO("Executing FetchEdges: %s", sentence_->toString().c_str());
    auto status = setupEdgeKeys();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    if (edgeKeys_.empty()) {
        onEmptyInputs();
    }

    fetchEdges();
}

Status FetchEdgesExecutor::setupEdgeKeys() {
    if (!edgeKeys_.empty()) {
        return Status::OK();
    }

    const auto *inputs = inputs_.get();
    if (!(sentence_->ref()->isInputExpr())) {
        auto *varInputs = ectx()->variableHolder()->get(varname_);
        if (varInputs == nullptr) {
            return Status::Error("Variable `%s' not defined", varname_.c_str());
        }
        DCHECK(inputs == nullptr);
        inputs = varInputs;
    }

    if (inputs == nullptr) {
        return Status::OK();
    }

    auto ret = inputs->getVIDs(*srcid_);
    if (!ret.ok()) {
        return ret.status();
    }
    auto srcVids = std::move(ret).value();

    ret = inputs->getVIDs(*dstid_);
    if (!ret.ok()) {
        return ret.status();
    }
    auto dstVids = std::move(ret).value();

    std::vector<int64_t> ranks;
    if (rank_ != nullptr) {
        ret = inputs->getVIDs(*rank_);
        if (!ret.ok()) {
            return ret.status();
        }
        ranks = std::move(ret).value();
    }

    for (auto index = 0u; index < srcVids.size(); ++index) {
        storage::cpp2::EdgeKey key;
        key.set_src(srcVids[index]);
        key.set_edge_type(edgeType_);
        key.set_dst(dstVids[index]);
        key.set_ranking(rank_ == nullptr ? 0 : ranks[index]);
        edgeKeys_.emplace_back(std::move(key));
    }

    return Status::OK();
}

void FetchEdgesExecutor::fetchEdges() {
    auto status = getPropNames();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(status.status());
        return;
    }

    auto props = status.value();
    auto future = ectx()->storage()->getEdgeProps(spaceId_, edgeKeys_, std::move(props));
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

StatusOr<std::vector<storage::cpp2::PropDef>> FetchEdgesExecutor::getPropNames() {
    std::vector<storage::cpp2::PropDef> props;
    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = prop.second;
        props.emplace_back(pd);
    }

    return props;
}

std::vector<std::string> FetchEdgesExecutor::getResultColumnNames() const {
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

void FetchEdgesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    for (auto &resp : all) {
        if (!resp.__isset.schema || !resp.__isset.data) {
            continue;
        }

        std::shared_ptr<ResultSchemaProvider> eschema;
        if (resp.get_schema() != nullptr) {
            eschema = std::make_shared<ResultSchemaProvider>(resp.schema);
        } else {
            continue;
        }

        auto *data = resp.get_data();
        if (data == nullptr || data->empty()) {
            continue;
        }
        RowSetReader rsReader(eschema, *data);
        auto iter = rsReader.begin();
        while (iter) {
            if (outputSchema == nullptr) {
                outputSchema = std::make_shared<SchemaWriter>();
                getOutputSchema(eschema.get(), &*iter, outputSchema.get());
                rsWriter = std::make_unique<RowSetWriter>(outputSchema);
            }

            auto collector = std::make_unique<Collector>(eschema.get());
            auto writer = std::make_unique<RowWriter>(outputSchema);

            auto &getters = expCtx_->getters();
            getters.getAliasProp = [&] (const std::string&, const std::string &prop) {
                return collector->collect(prop, &*iter, writer.get());
            };
            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval();
            }

            rsWriter->addRow(*writer);
            ++iter;
        }  // while `iter'
    }  // for `resp'

    finishExecution(std::move(rsWriter));
}

void FetchEdgesExecutor::finishExecution(std::unique_ptr<RowSetWriter> rsWriter) {
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

void FetchEdgesExecutor::getOutputSchema(
        meta::SchemaProviderIf *schema,
        const RowReader *reader,
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

void FetchEdgesExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    inputs_ = std::move(result);
}

void FetchEdgesExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void FetchEdgesExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}
}  // namespace graph
}  // namespace nebula
