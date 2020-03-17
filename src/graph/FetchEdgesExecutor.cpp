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
        : FetchExecutor(ectx, "fetch_edges") {
    sentence_ = static_cast<FetchEdgesSentence*>(sentence);
}

Status FetchEdgesExecutor::prepare() {
    return Status::OK();
}

Status FetchEdgesExecutor::prepareClauses() {
    Status status = Status::OK();

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        spaceId_ = ectx()->rctx()->session()->space();
        expCtx_ = std::make_unique<ExpressionContext>();
        expCtx_->setStorageClient(ectx()->getStorageClient());
        expCtx_->setSpace(spaceId_);
        yieldClause_ = DCHECK_NOTNULL(sentence_)->yieldClause();
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

        // Add SrcID, DstID, Rank before prepareYield()
        edgeSrcName_ = *labelName_ + "._src";
        edgeDstName_ = *labelName_ + "._dst";
        edgeRankName_ = *labelName_ + "._rank";
        returnColNames_.emplace_back(edgeSrcName_);
        returnColNames_.emplace_back(edgeDstName_);
        returnColNames_.emplace_back(edgeRankName_);
        status = prepareYield();
        if (!status.ok()) {
            LOG(ERROR) << "Prepare yield failed: " << status;
            break;
        }
        status = checkEdgeProps();
        if (!status.ok()) {
            LOG(ERROR) << "Check edge props failed: " << status;
            break;
        }
    } while (false);
    return status;
}

Status FetchEdgesExecutor::checkEdgeProps() {
    auto aliasProps = expCtx_->aliasProps();
    for (auto &pair : aliasProps) {
        if (pair.first != *labelName_) {
            return Status::SyntaxError(
                "Near [%s.%s], edge should be declared in `ON' clause first.",
                    pair.first.c_str(), pair.second.c_str());
        }
        auto propName = pair.second;
        if (propName == _SRC || propName == _DST
                || propName == _RANK || propName == _TYPE) {
            continue;
        }
        if (labelSchema_->getFieldIndex(propName) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                    propName.c_str(), pair.first.c_str());
        }
    }
    return Status::OK();
}

void FetchEdgesExecutor::execute() {
    FLOG_INFO("Executing FetchEdges: %s", sentence_->toString().c_str());
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto ret = getEdgeKeys(expCtx_.get(), sentence_, edgeType_, distinct_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        doError(std::move(ret).status());
        return;
    }

    edgeKeys_ = std::move(ret).value();
    if (edgeKeys_.empty()) {
        onEmptyInputs();
        return;
    }

    fetchEdges();
}

void FetchEdgesExecutor::fetchEdges() {
    std::vector<storage::cpp2::PropDef> props;
    auto status = getPropNames(props);
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    if (props.empty()) {
        LOG(WARNING) << "Empty props of tag " << labelName_;
        doEmptyResp();
        return;
    }

    auto future = ectx()->getStorageClient()->getEdgeProps(spaceId_, edgeKeys_, std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (RpcResponse &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get edge `%s' props failed", sentence_->edge()->c_str()));
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
        auto msg = folly::stringPrintf("Get edge `%s' props faield: %s.",
                sentence_->edge()->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
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
    Getters getters;
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
            outputSchema->appendCol(edgeSrcName_, nebula::cpp2::SupportedType::VID);
            outputSchema->appendCol(edgeDstName_, nebula::cpp2::SupportedType::VID);
            outputSchema->appendCol(edgeRankName_, nebula::cpp2::SupportedType::INT);
            auto status = getOutputSchema(eschema.get(), &*iter, outputSchema.get());
            if (!status.ok()) {
                LOG(ERROR) << "Get output schema failed: " << status;
                doError(Status::Error("Get output schema failed: %s.", status.toString().c_str()));
                return;
            }
            rsWriter = std::make_unique<RowSetWriter>(outputSchema);
        }
        while (iter) {
            auto writer = std::make_unique<RowWriter>(outputSchema);
            auto src = Collector::getProp(eschema.get(), _SRC, &*iter);
            auto dst = Collector::getProp(eschema.get(), _DST, &*iter);
            auto rank = Collector::getProp(eschema.get(), _RANK, &*iter);
            if (!src.ok() || !dst.ok() || !rank.ok()) {
                LOG(ERROR) << "Get edge key failed";
                doError(Status::Error("Get edge key failed"));
                return;
            }
            (*writer) << boost::get<int64_t>(src.value())
                      << boost::get<int64_t>(dst.value())
                      << boost::get<int64_t>(rank.value());

            getters.getEdgeDstId = [&iter,
                                    &eschema] (const std::string&) -> OptVariantType {
                return Collector::getProp(eschema.get(), "_dst", &*iter);
            };
            getters.getAliasProp =
                [&iter, &eschema] (const std::string&,
                                   const std::string &prop) -> OptVariantType {
                return Collector::getProp(eschema.get(), prop, &*iter);
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

            std::string encode = writer->encode();
            rsWriter->addRow(std::move(encode));
            ++iter;
        }  // while `iter'
    }  // for `resp'

    finishExecution(std::move(rsWriter));
}

}  // namespace graph
}  // namespace nebula
