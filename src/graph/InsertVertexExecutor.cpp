/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InsertVertexExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

InsertVertexExecutor::InsertVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertVertexSentence*>(sentence);
}


Status InsertVertexExecutor::prepare() {
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        rows_ = sentence_->rows();
        if (rows_.empty()) {
            status = Status::Error("VALUES cannot be empty");
            break;
        }

        auto tagItems = sentence_->tagItems();
        overwritable_ = sentence_->overwritable();
        spaceId_ = ectx()->rctx()->session()->space();

        tagIds_.reserve(tagItems.size());
        schemas_.reserve(tagItems.size());
        tagProps_.reserve(tagItems.size());

        for (auto& item : tagItems) {
            auto *tagName = item->tagName();
            auto tagStatus = ectx()->schemaManager()->toTagID(spaceId_, *tagName);
            if (!tagStatus.ok()) {
                return Status::Error("No schema found for `%s'", tagName->c_str());
            }

            auto tagId = tagStatus.value();
            auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
            if (schema == nullptr) {
                return Status::Error("No schema found for `%s'", tagName->c_str());
            }

            auto props = item->properties();
            // Now default value is unsupported, props should equal to schema's fields
            if (schema->getNumFields() != props.size()) {
                LOG(ERROR) << "props number " << props.size()
                           << ", schema field number " << schema->getNumFields();
                return Status::Error("Wrong number of props");
            }

            tagIds_.emplace_back(tagId);
            schemas_.emplace_back(schema);
            tagProps_.emplace_back(props);

            // Check field name
            auto checkStatus = checkFieldName(schema, props);
            if (!checkStatus.ok()) {
                return checkStatus;
            }
        }
    } while (false);

    return status;
}


StatusOr<std::vector<storage::cpp2::Vertex>> InsertVertexExecutor::prepareVertices() {
    std::vector<storage::cpp2::Vertex> vertices(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto id = row->id();
        auto expressions = row->values();

        std::vector<VariantType> values;
        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            values.emplace_back(expr->eval());
        }

        storage::cpp2::Vertex vertex;
        std::vector<storage::cpp2::Tag> tags(tagIds_.size());

        auto valuePos = 0u;
        for (auto index = 0u; index < tagIds_.size(); index++) {
            auto &tag = tags[index];
            auto tagId = tagIds_[index];
            auto propSize = tagProps_[index].size();
            auto schema = schemas_[index];

            // props's number should equal to value's number
            if ((valuePos + propSize) > values.size()) {
                LOG(ERROR) << "Input values number " << values.size()
                           << ", props number " << propSize;
                return Status::Error("Wrong number of value");
            }

            RowWriter writer(schema);
            auto valueIndex = valuePos;
            for (auto fieldIndex = 0u; fieldIndex < schema->getNumFields(); fieldIndex++) {
                auto& value = values[valueIndex];

                // Check value type
                auto schemaType = schema->getFieldType(fieldIndex);
                if (!checkValueType(schema->getFieldType(fieldIndex), value)) {
                    LOG(ERROR) << "ValueType is wrong, schema type "
                               << static_cast<int32_t>(schemaType.type)
                               << ", input type " <<  value.which();
                    return Status::Error("ValueType is wrong");
                }
                writeVariantType(writer, value);
                valueIndex++;
            }

            tag.set_tag_id(tagId);
            tag.set_props(writer.encode());
            valuePos += propSize;
        }

        vertex.set_id(id);
        vertex.set_tags(std::move(tags));
        vertices.emplace_back(std::move(vertex));
    }

    return vertices;
}


void InsertVertexExecutor::execute() {
    auto result = prepareVertices();
    if (!result.ok()) {
        DCHECK(onError_);
        onError_(std::move(result).status());
        return;
    }
    auto future = ectx()->storage()->addVertices(spaceId_,
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
