/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InsertVertexExecutor.h"
#include "storage/client/StorageClient.h"
#include "meta/NebulaSchemaProvider.h"

namespace nebula {
namespace graph {

using nebula::meta::NebulaSchemaProvider;

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
            auto fields = schema->getFieldNames();
            // Now default value is unsupported, props should equal to schema's fields
            if (schema->getNumFields() != props.size()) {
                auto *mc = ectx()->getMetaClient();

                std::vector<std::string> properties;
                for (auto& prop : props) {
                    properties.emplace_back(*prop);
                }

                for (size_t i = 0; i < schema->getNumFields(); i++) {
                    auto name = folly::stringPrintf("%s", schema->getFieldName(i));
                    auto it = std::find(properties.begin(), properties.end(), name);
                    if (it != properties.end()) {
                        LOG(INFO) << "Unfind Name: " << name;
                    }
                }

                for (auto& name : fields) {
                    LOG(INFO) << "Name: " << name;
                }
                for (auto& prop : props) {
                    LOG(INFO) << "Prop: " << *prop;
                }

                std::sort(begin(fields), end(fields));
                std::sort(begin(properties), end(properties));

                std::vector<std::string> diff(fields.size());
                auto iter = std::set_difference(begin(fields), end(fields), begin(properties),
                                                end(properties), diff.begin());
                diff.resize(iter - diff.begin());
                LOG(INFO) << "---------";
                for (auto ele : diff) {
                    LOG(INFO) << "Ele: " << ele;
                    auto type = schema->getFieldType(ele).type;
                    if (mc->hasTagDefaultValue(spaceId_, tagId, ele)) {
                        switch (type) {
                            case nebula::cpp2::SupportedType::BOOL:
                                LOG(INFO) << "Ele: " << ele << " bool default value "
                                          << mc->getTagBoolDefaultValue(spaceId_, tagId).value();
                                break;
                            case nebula::cpp2::SupportedType::INT:
                                LOG(INFO) << "Ele: " << ele << " int default value "
                                          << mc->getTagIntDefultValue(spaceId_, tagId).value();
                                break;
                            case nebula::cpp2::SupportedType::DOUBLE:
                                LOG(INFO) << "Ele: " << ele << " double default value "
                                          << mc->getTagDoublelDefaultValue(spaceId_, tagId).value();
                                break;
                            case nebula::cpp2::SupportedType::STRING:
                                LOG(INFO) << "Ele: " << ele << " string default value "
                                          << mc->getTagStringDefaultValue(spaceId_, tagId).value();
                                break;
                            default:
                                break;
                        }
                    } else {
                        LOG(ERROR) << "No default value";
                        return Status::Error("No default value");
                    }
                }


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
        auto status = row->id()->prepare();
        if (!status.ok()) {
            return status;
        }
        auto v = row->id()->eval();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        auto id = Expression::asInt(v);
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
                if (!checkValueType(schemaType, value)) {
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
