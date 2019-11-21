/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InsertEdgeExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

InsertEdgeExecutor::InsertEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertEdgeSentence*>(sentence);
}


Status InsertEdgeExecutor::prepare() {
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    return Status::OK();
}


Status InsertEdgeExecutor::check() {
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        auto spaceId = ectx()->rctx()->session()->space();
        overwritable_ = sentence_->overwritable();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *sentence_->edge());
        if (!edgeStatus.ok()) {
            status = edgeStatus.status();
            break;
        }
        edgeType_ = edgeStatus.value();
        auto props = sentence_->properties();
        rows_ = sentence_->rows();

        expCtx_->setStorageClient(ectx()->getStorageClient());

        schema_ = ectx()->schemaManager()->getEdgeSchema(spaceId, edgeType_);
        if (schema_ == nullptr) {
            status = Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
            break;
        }

        // Now default value is unsupported
        if (props.size() != schema_->getNumFields()) {
            LOG(ERROR) << "Input props number " << props.size()
                       << ", schema fields number " << schema_->getNumFields();
            status = Status::Error("Wrong number of props");
            break;
        }

        // Check field name
        auto checkStatus = checkFieldName(schema_, props);
        if (!checkStatus.ok()) {
            status = checkStatus.status();
            break;
        }

        schemaIndexes_ = std::move(checkStatus).value();
    } while (false);

    if (!status.ok()) {
        LOG(ERROR) << status;
        return status;
    }

    return status;
}


StatusOr<std::vector<storage::cpp2::Edge>> InsertEdgeExecutor::prepareEdges() {
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());

    auto space = ectx()->rctx()->session()->space();
    expCtx_->setSpace(space);

    std::vector<storage::cpp2::Edge> edges(rows_.size() * 2);   // inbound and outbound
    auto index = 0;
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto sid = row->srcid();
        sid->setContext(expCtx_.get());
        auto status = sid->prepare();
        if (!status.ok()) {
            return status;
        }
        auto ovalue = sid->eval();
        if (!ovalue.ok()) {
            return ovalue.status();
        }

        auto v = ovalue.value();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        auto src = Expression::asInt(v);

        auto did = row->dstid();
        did->setContext(expCtx_.get());
        status = did->prepare();
        if (!status.ok()) {
            return status;
        }
        ovalue = did->eval();
        if (!ovalue.ok()) {
            return ovalue.status();
        }

        v = ovalue.value();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        auto dst = Expression::asInt(v);

        int64_t rank = row->rank();

        auto expressions = row->values();

        // Now default value is unsupported
        if (expressions.size() != schema_->getNumFields()) {
            LOG(ERROR) << "Input values number " << expressions.size()
                       << ", schema field number " << schema_->getNumFields();
            return Status::Error("Wrong number of values");
        }

        std::vector<VariantType> values;
        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            status = expr->prepare();
            if (!status.ok()) {
                return status;
            }

            ovalue = expr->eval();
            if (!ovalue.ok()) {
                return ovalue.status();
            }
            values.emplace_back(ovalue.value());
        }

        RowWriter writer(schema_);
        auto iter = schema_->begin();
        while (iter) {
            // Check value type
            auto schemaType = iter->getType();
            auto &value = values[schemaIndexes_[iter->getName()]];
            if (!checkValueType(schemaType, value)) {
                auto *output = "ValueType is wrong, schema type [%d], "
                                "input type [%d], near `%s'";
                auto error = folly::stringPrintf(output, static_cast<int32_t>(schemaType.type),
                        value.which(), iter->getName());
                LOG(ERROR) << error;
                return Status::Error(std::move(error));
            }

            if (schemaType.type == nebula::cpp2::SupportedType::TIMESTAMP) {
                auto timestamp = toTimestamp(value);
                if (!timestamp.ok()) {
                    return timestamp.status();
                }
                writeVariantType(writer, timestamp.value());
            } else {
                writeVariantType(writer, value);
            }
            ++iter;
        }
        {
            auto &out = edges[index++];
            out.key.set_src(src);
            out.key.set_dst(dst);
            out.key.set_ranking(rank);
            out.key.set_edge_type(edgeType_);
            out.props = writer.encode();
            out.__isset.key = true;
            out.__isset.props = true;
        }
        {
            auto &in = edges[index++];
            in.key.set_src(dst);
            in.key.set_dst(src);
            in.key.set_ranking(rank);
            in.key.set_edge_type(-edgeType_);
            in.props = "";
            in.__isset.key = true;
            in.__isset.props = true;
        }
    }

    return edges;
}


void InsertEdgeExecutor::execute() {
    auto status = check();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto result = prepareEdges();
    if (!result.ok()) {
        DCHECK(onError_);
        onError_(std::move(result).status());
        return;
    }
    auto space = ectx()->rctx()->session()->space();
    auto future = ectx()->getStorageClient()->addEdges(space,
                                                       std::move(result).value(),
                                                       overwritable_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        // For insertion, we regard partial success as failure.
        auto completeness = resp.completeness();
        if (completeness != 100) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
