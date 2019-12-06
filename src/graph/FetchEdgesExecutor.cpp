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
        : FetchExecutor(ectx) {
    sentence_ = static_cast<FetchEdgesSentence*>(sentence);
}

Status FetchEdgesExecutor::prepare() {
    return Status::OK();
}

Status FetchEdgesExecutor::prepareClauses() {
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
        labelName_ = sentence_->edge();
        auto result = ectx()->schemaManager()->toEdgeType(spaceId_, *labelName_);
        if (!result.ok()) {
            status = result.status();
            break;
        }
        edgeType_ = result.value();
        labelSchema_ = ectx()->schemaManager()->getEdgeSchema(spaceId_, edgeType_);
        if (labelSchema_ == nullptr) {
            LOG(ERROR) << *labelName_ << " edge schema not exist.";
            status = Status::Error("%s edge schema not exist.", labelName_->c_str());
            break;
        }

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
        if (sentence_->isRef()) {
            auto *edgeKeyRef = sentence_->ref();

            srcid_ = edgeKeyRef->srcid();
            if (srcid_ == nullptr) {
                status = Status::Error("Internal error.");
                LOG(ERROR) << "Get src nullptr.";
                break;
            }

            dstid_ = edgeKeyRef->dstid();
            if (dstid_ == nullptr) {
                status = Status::Error("Internal error.");
                LOG(ERROR) << "Get dst nullptr.";
                break;
            }

            rank_ = edgeKeyRef->rank();

            if ((*srcid_ == "*")
                    || (*dstid_ == "*")
                    || (rank_ != nullptr && *rank_ == "*")) {
                status = Status::Error("Can not use `*' to reference a vertex id column.");
                break;
            }

            auto ret = edgeKeyRef->varname();
            if (!ret.ok()) {
                status = std::move(ret).status();
                break;
            }
            varname_ = std::move(ret).value();
        }
    } while (false);

    return status;
}

void FetchEdgesExecutor::execute() {
    FLOG_INFO("Executing FetchEdges: %s", sentence_->toString().c_str());
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getFetchEdgesStats());
        return;
    }
    status = setupEdgeKeys();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getFetchEdgesStats());
        return;
    }

    if (edgeKeys_.empty()) {
        onEmptyInputs();
        return;
    }

    fetchEdges();
}

Status FetchEdgesExecutor::setupEdgeKeys() {
    Status status = Status::OK();
    hash_ = [] (const storage::cpp2::EdgeKey &key) -> size_t {
            return std::hash<VertexID>()(key.src)
                    ^ std::hash<VertexID>()(key.dst)
                    ^ std::hash<EdgeRanking>()(key.ranking);
    };
    if (sentence_->isRef()) {
        status = setupEdgeKeysFromRef();
    } else {
        status = setupEdgeKeysFromExpr();
    }

    VLOG(3) << "EdgeKey length: " << edgeKeys_.size();

    return status;
}

Status FetchEdgesExecutor::setupEdgeKeysFromRef() {
    const InterimResult *inputs;
    if (sentence_->ref()->isInputExpr()) {
        inputs = inputs_.get();
    } else {
        bool existing = false;
        inputs = ectx()->variableHolder()->get(varname_, &existing);
        if (!existing) {
            return Status::Error("Variable `%s' not defined", varname_.c_str());
        }
    }
    if (inputs == nullptr || !inputs->hasData()) {
        // we have empty imputs from pipe.
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

    std::vector<EdgeRanking> ranks;
    if (rank_ != nullptr) {
        ret = inputs->getVIDs(*rank_);
        if (!ret.ok()) {
            return ret.status();
        }
        ranks = std::move(ret).value();
    }

    std::unique_ptr<EdgeKeyHashSet> uniq;
    if (distinct_) {
        uniq = std::make_unique<EdgeKeyHashSet>(256, hash_);
    }
    for (decltype(srcVids.size()) index = 0u; index < srcVids.size(); ++index) {
        storage::cpp2::EdgeKey key;
        key.set_src(srcVids[index]);
        key.set_edge_type(edgeType_);
        key.set_dst(dstVids[index]);
        key.set_ranking(rank_ == nullptr ? 0 : ranks[index]);

        if (distinct_) {
            auto result = uniq->emplace(key);
            if (result.second) {
                edgeKeys_.emplace_back(std::move(key));
            }
        } else {
            edgeKeys_.emplace_back(std::move(key));
        }
    }

    return Status::OK();
}

Status FetchEdgesExecutor::setupEdgeKeysFromExpr() {
    Status status = Status::OK();
    std::unique_ptr<EdgeKeyHashSet> uniq;
    if (distinct_) {
        uniq = std::make_unique<EdgeKeyHashSet>(256, hash_);
    }

    auto edgeKeyExprs = sentence_->keys()->keys();
    expCtx_->setSpace(spaceId_);

    for (auto *keyExpr : edgeKeyExprs) {
        auto *srcExpr = keyExpr->srcid();
        srcExpr->setContext(expCtx_.get());

        auto *dstExpr = keyExpr->dstid();
        dstExpr->setContext(expCtx_.get());

        auto rank = keyExpr->rank();
        status = srcExpr->prepare();
        if (!status.ok()) {
            break;
        }
        status = dstExpr->prepare();
        if (!status.ok()) {
            break;
        }
        auto value = srcExpr->eval();
        if (!value.ok()) {
            return value.status();
        }
        auto srcid = value.value();
        value = dstExpr->eval();
        if (!value.ok()) {
            return value.status();
        }
        auto dstid = value.value();
        if (!Expression::isInt(srcid) || !Expression::isInt(dstid)) {
            status = Status::Error("ID should be of type integer.");
            break;
        }
        storage::cpp2::EdgeKey key;
        key.set_src(Expression::asInt(srcid));
        key.set_edge_type(edgeType_);
        key.set_dst(Expression::asInt(dstid));
        key.set_ranking(rank);

        if (distinct_) {
            auto ret = uniq->emplace(key);
            if (ret.second) {
                edgeKeys_.emplace_back(std::move(key));
            }
        } else {
            edgeKeys_.emplace_back(std::move(key));
        }
    }

    return status;
}

void FetchEdgesExecutor::fetchEdges() {
    std::vector<storage::cpp2::PropDef> props;
    auto status = getPropNames(props);
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getFetchEdgesStats());
        return;
    }

    if (props.empty()) {
        doError(Status::Error("No props declared."),
                 ectx()->getGraphStats()->getFetchEdgesStats());
        return;
    }

    auto future = ectx()->getStorageClient()->getEdgeProps(spaceId_, edgeKeys_, std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get props failed"),
                    ectx()->getGraphStats()->getFetchEdgesStats());
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get edges partially failed: "  << completeness << "%";
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
        doError(Status::Error("Internal error"),
                ectx()->getGraphStats()->getFetchEdgesStats());
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

Status FetchEdgesExecutor::getPropNames(std::vector<storage::cpp2::PropDef> &props) {
    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = prop.second;
        auto status = ectx()->schemaManager()->toEdgeType(spaceId_, prop.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", prop.first.c_str());
        }
        auto edgeType = status.value();
        pd.id.set_edge_type(edgeType);
        props.emplace_back(std::move(pd));
    }

    return Status::OK();
}

void FetchEdgesExecutor::processResult(RpcResponse &&result) {
    auto all = result.responses();
    std::shared_ptr<SchemaWriter> outputSchema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto uniqResult = std::make_unique<std::unordered_set<std::string>>();
    for (auto &resp : all) {
        if (!resp.__isset.schema || !resp.__isset.data
                || resp.get_schema() == nullptr || resp.get_data() == nullptr
                || resp.data.empty()) {
            continue;
        }

        auto eschema = std::make_shared<ResultSchemaProvider>(*(resp.get_schema()));
        RowSetReader rsReader(eschema, *(resp.get_data()));
        auto iter = rsReader.begin();
        if (outputSchema == nullptr) {
            outputSchema = std::make_shared<SchemaWriter>();
            auto status = getOutputSchema(eschema.get(), &*iter, outputSchema.get());
            if (!status.ok()) {
                LOG(ERROR) << "Get getOutputSchema failed: " << status;
                doError(Status::Error("Internal error."),
                        ectx()->getGraphStats()->getFetchEdgesStats());
                return;
            }
            rsWriter = std::make_unique<RowSetWriter>(outputSchema);
        }
        while (iter) {
            auto writer = std::make_unique<RowWriter>(outputSchema);

            auto &getters = expCtx_->getters();
            getters.getAliasProp =
                [&iter, &eschema] (const std::string&,
                                   const std::string &prop) -> OptVariantType {
                return Collector::getProp(eschema.get(), prop, &*iter);
            };
            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval();
                if (!value.ok()) {
                    doError(std::move(value).status(),
                             ectx()->getGraphStats()->getFetchEdgesStats());
                    return;
                }
                auto status = Collector::collect(value.value(), writer.get());
                if (!status.ok()) {
                    LOG(ERROR) << "Collect prop error: " << status;
                    doError(std::move(status), ectx()->getGraphStats()->getFetchEdgesStats());
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
            ++iter;
        }  // while `iter'
    }  // for `resp'

    finishExecution(std::move(rsWriter));
}

}  // namespace graph
}  // namespace nebula
