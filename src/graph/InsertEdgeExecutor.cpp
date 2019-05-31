/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InsertEdgeExecutor.h"
#include "meta/SchemaManager.h"
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
        properties_ = sentence_->properties();
        rows_ = sentence_->rows();
        schema_ = ectx()->schemaManager()->getEdgeSchema(spaceId, edgeType_);
        if (schema_ == nullptr) {
            status = Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
            break;
        }
    } while (false);

    return status;
}


StatusOr<std::vector<storage::cpp2::Edge>> InsertEdgeExecutor::prepareEdges() {
    std::vector<storage::cpp2::Edge> edges(rows_.size() * 2);   // inbound and outbound
    auto index = 0;
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto src = row->srcid();
        auto dst = row->dstid();
        auto rank = row->rank();
        auto expressions = row->values();

        // Now default value is unsupported
        if (expressions.size() != schema_->getNumFields() ||
            properties_.size() != schema_->getNumFields()) {
            LOG(ERROR) << "Input values number " << expressions.size()
                       << ", props number " << properties_.size()
                       << ", schema field number " << schema_->getNumFields();
            return Status::Error("Wrong number of fields");
        }

        std::vector<VariantType> values;
        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            values.emplace_back(expr->eval());
        }

        RowWriter writer(schema_);
        auto schemaIndex = 0u;
        for (auto &value : values) {
            // Check field name
            auto schemaFileName = schema_->getFieldName(schemaIndex);
            if (schemaFileName != *properties_[schemaIndex]) {
                LOG(ERROR) << "Field name is wrong, schema field " << schemaFileName
                           << ", input field " << *properties_[schemaIndex];
                return Status::Error("Input field name `%s' is wrong",
                                      properties_[schemaIndex]->c_str());
            }
            // Check value type
            auto schemaType = valueTypeToString(schema_->getFieldType(schemaIndex));
            auto propType = variantTypeToString(value);
            if (schemaType != propType) {
                DCHECK(onError_);
                LOG(ERROR) << "ValueType is wrong, schema type " << schemaType
                           << ", input type " <<  propType;
                return Status::Error("ValueType is wrong, schema type `%s', input type `%s'",
                                      schemaType.c_str(), propType.c_str());
            }
            writeVariantType(writer, value);
            schemaIndex++;
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
