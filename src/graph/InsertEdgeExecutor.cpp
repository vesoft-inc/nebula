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
            status = checkStatus;
            break;
        }
    } while (false);

    return status;
}


StatusOr<std::vector<storage::cpp2::Edge>> InsertEdgeExecutor::prepareEdges() {
    auto status = Status::OK();
    std::vector<storage::cpp2::Edge> edges(rows_.size() * 2);   // inbound and outbound
    auto index = 0;
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        status = row->srcid()->prepare();
        if (!status.ok()) {
            break;
        }
        auto v = row->srcid()->eval();
        if (!Expression::isInt(v)) {
            status = Status::Error("Vertex ID should be of type integer");
            break;
        }
        auto src = Expression::asInt(v);
        status = row->dstid()->prepare();
        if (!status.ok()) {
            break;
        }
        v = row->dstid()->eval();
        if (!Expression::isInt(v)) {
            status = Status::Error("Vertex ID should be of type integer");
            break;
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
            values.emplace_back(expr->eval());
        }

        RowWriter writer(schema_);
        auto fieldIndex = 0u;
        for (auto &value : values) {
            // Check value type
            auto schemaType = schema_->getFieldType(fieldIndex);
            if (!checkValueType(schemaType, value)) {
                DCHECK(onError_);
                LOG(ERROR) << "ValueType is wrong, schema type "
                           << static_cast<int32_t>(schemaType.type)
                           << ", input type " <<  value.which();
                return Status::Error("ValueType is wrong");
            }
            writeVariantType(writer, value);
            fieldIndex++;
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

    if (!status.ok()) {
        return status;
    }

    return edges;
}


void InsertEdgeExecutor::execute() {
    auto result = prepareEdges();
    if (!result.ok()) {
        DCHECK(onError_);
        onError_(std::move(result).status());
        return;
    }
    auto space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->addEdges(space, std::move(result).value(), overwritable_);
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
        onFinish_();
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
