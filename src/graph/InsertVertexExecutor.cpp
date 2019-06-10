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

        tagItems_ = sentence_->tagItems();
        if (tagItems_.size() == 0) {
            status = Status::Error("Tag items empty");
            break;
        }
        overwritable_ = sentence_->overwritable();
        spaceId_ = ectx()->rctx()->session()->space();
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
        std::vector<storage::cpp2::Tag> tags(tagItems_.size());
        // The index for multi tags
        auto tagIndex = 0u;
        auto valuePos = 0u;
        for (auto& it : tagItems_) {
            auto &tag = tags[tagIndex];
            auto *tagName = it->tagName();
            auto status = ectx()->schemaManager()->toTagID(spaceId_, *tagName);
            if (!status.ok()) {
                return Status::Error("No schema found for `%s'", tagName->c_str());
            }

            auto tagId = status.value();
            auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
            if (schema == nullptr) {
                return Status::Error("No schema found for `%s'", tagName->c_str());
            }

            auto props = it->properties();
            // Now default value is unsupported, props should equal to schema's fields
            if (schema->getNumFields() != props.size() ||
                (valuePos + props.size()) > values.size()) {
                LOG(ERROR) << "Input values number " << values.size()
                           << ", props number " << props.size()
                           << ", schema field number " << schema->getNumFields();
                return Status::Error("Wrong number of fields");
            }

            RowWriter writer(schema);
            auto valueIndex = valuePos;
            for (auto schemaIndex = 0u; schemaIndex < schema->getNumFields(); schemaIndex++) {
                auto& value = values[valueIndex];

                // Check field name
                std::string schemaFileName = schema->getFieldName(schemaIndex);
                if (schemaFileName != *props[schemaIndex]) {
                    LOG(ERROR) << "Field is wrong, schema field " << schemaFileName
                               << ", input field " << *props[schemaIndex];
                    return Status::Error("Input field name `%s' is wrong",
                                         props[schemaIndex]->c_str());
                }

                // Check value type
                auto schemaType = valueTypeToString(schema->getFieldType(schemaIndex));
                auto propType = variantTypeToString(value);
                if (schemaType != propType) {
                    LOG(ERROR) << "ValueType is wrong, schema type " << schemaType
                               << ", input type " <<  propType;
                    return Status::Error("ValueType is wrong, schema type `%s', input type `%s'",
                                         schemaType.c_str(), propType.c_str());
                }
                writeVariantType(writer, value);
                valueIndex++;
            }

            tag.set_tag_id(tagId);
            tag.set_props(writer.encode());
            tagIndex++;
            valuePos += props.size();
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
