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

InsertVertexExecutor::InsertVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertVertexSentence*>(sentence);
}


Status InsertVertexExecutor::prepare() {
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    return Status::OK();
}


Status InsertVertexExecutor::check() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        return status;
    }

    rows_ = sentence_->rows();
    if (rows_.empty()) {
        return Status::Error("VALUES cannot be empty");
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
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        auto tagId = tagStatus.value();
        auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        auto props = item->properties();
        if (props.size() > schema->getNumFields()) {
            LOG(ERROR) << "Input props number "
                       << props.size()
                       << ", schema fields number "
                       << schema->getNumFields();
            return Status::Error("Wrong number of props");
        }

        if (schema->getNumFields() != props.size()) {
            auto *mc = ectx()->getMetaClient();

            for (size_t i = 0; i < schema->getNumFields(); i++) {
                std::string name = schema->getFieldName(i);
                auto it = std::find_if(props.begin(), props.end(),
                                       [name](std::string *prop) { return *prop == name;});

                if (it == props.end() && defaultValues_.find(name) == defaultValues_.end()) {
                    auto valueResult = mc->getTagDefaultValue(spaceId_, tagId, name).get();
                    if (!valueResult.ok()) {
                        LOG(ERROR) << "Not exist default value: " << name;
                        return Status::Error("Not exist default value");
                    } else {
                        VLOG(3) << "Default Value: " << name << ":" << valueResult.value();
                        defaultValues_.emplace(name, valueResult.value());
                    }
                }
            }
        } else {
            // Check field name
            auto checkStatus = checkFieldName(schema, props);
            if (!checkStatus.ok()) {
                LOG(ERROR) << "Check Status Failed: " << checkStatus;
                return checkStatus;
            }
        }

        tagIds_.emplace_back(tagId);
        schemas_.emplace_back(schema);
        tagProps_.emplace_back(props);
    }
    return Status::OK();
}

StatusOr<std::vector<storage::cpp2::Vertex>> InsertVertexExecutor::prepareVertices() {
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);

    std::vector<storage::cpp2::Vertex> vertices(rows_.size());
    for (size_t i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto rid = row->id();
        rid->setContext(expCtx_.get());

        auto status = rid->prepare();
        if (!status.ok()) {
            return status;
        }
        auto ovalue = rid->eval();
        if (!ovalue.ok()) {
            return ovalue.status();
        }

        auto v = ovalue.value();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        auto id = Expression::asInt(v);
        auto expressions = row->values();

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

        storage::cpp2::Vertex vertex;
        std::vector<storage::cpp2::Tag> tags(tagIds_.size());

        auto valuePos = 0u;
        int32_t insertPosition = 0;
        for (auto index = 0u; index < tagIds_.size(); index++) {
            auto &tag = tags[index];
            auto tagId = tagIds_[index];
            auto props = tagProps_[index];
            auto schema = schemas_[index];

            auto schemaNumFields = schema->getNumFields();
            size_t propsIndex = 0;
            for (size_t schemaIndex = 0; schemaIndex < schemaNumFields; schemaIndex++) {
                auto fieldName = schema->getFieldName(schemaIndex);

                if ((propsIndex < props.size() && *props[propsIndex] != fieldName) ||
                    (propsIndex >= props.size())) {
                    // fetch default value from cache
                    auto type = schema->getFieldType(schemaIndex).type;
                    VariantType variantType;
                    switch (type) {
                        case nebula::cpp2::SupportedType::BOOL:
                            try {
                                variantType = folly::to<bool>(defaultValues_[fieldName]);
                            } catch (const std::exception& ex) {
                                LOG(ERROR) << "Conversion to bool failed: "
                                           << defaultValues_[fieldName];
                                return Status::Error("Type Conversion Failed");
                            }

                            VLOG(3) << "Insert " << fieldName << ":" << variantType
                                    << " at " << insertPosition;
                            values.insert(values.begin() + insertPosition, std::move(variantType));
                            break;
                        case nebula::cpp2::SupportedType::INT:
                            try {
                                variantType = folly::to<int64_t>(defaultValues_[fieldName]);
                            } catch (const std::exception& ex) {
                                LOG(ERROR) << "Conversion to int64_t failed: "
                                           << defaultValues_[fieldName];
                                return Status::Error("Type Conversion Failed");
                            }

                            VLOG(3) << "Insert " << fieldName << ":" << variantType
                                    << " at " << insertPosition;
                            values.insert(values.begin() + insertPosition, std::move(variantType));
                            break;
                        case nebula::cpp2::SupportedType::DOUBLE:
                            try {
                                variantType = folly::to<double>(defaultValues_[fieldName]);
                            } catch (const std::exception& ex) {
                                LOG(ERROR) << "Conversion to double failed: "
                                           << defaultValues_[fieldName];
                                return Status::Error("Type Conversion Failed");
                            }

                            VLOG(3) << "Insert " << fieldName << ":" << variantType
                                    << " at " << insertPosition;
                            values.insert(values.begin() + insertPosition, std::move(variantType));
                            break;
                        case nebula::cpp2::SupportedType::STRING:
                            variantType = defaultValues_[fieldName];
                            values.insert(values.begin() + insertPosition, std::move(variantType));
                            break;
                        default:
                            LOG(ERROR) << "Unknow type";
                            return Status::Error("Unknow type");
                    }
                    insertPosition++;
                } else if (*props[propsIndex] == fieldName) {
                    propsIndex++;
                    insertPosition++;
                } else {
                    insertPosition++;
                }
            }

            RowWriter writer(schema);
            auto valueIndex = valuePos;
            for (size_t fieldIndex = 0u; fieldIndex < schemaNumFields; fieldIndex++) {
                auto& value = values[valueIndex];

                // Check value type
                auto schemaType = schema->getFieldType(fieldIndex);
                if (!checkValueType(schemaType, value)) {
                    LOG(ERROR) << "ValueType is wrong, schema type "
                               << static_cast<int32_t>(schemaType.type)
                               << ", input type " <<  value.which();
                    return Status::Error("ValueType is wrong");
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

                valueIndex++;
            }

            tag.set_tag_id(tagId);
            tag.set_props(writer.encode());
            valuePos += schemaNumFields;
        }

        vertex.set_id(id);
        vertex.set_tags(std::move(tags));
        vertices.emplace_back(std::move(vertex));
    }

    return vertices;
}


void InsertVertexExecutor::execute() {
    auto status = check();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto result = prepareVertices();
    if (!result.ok()) {
        DCHECK(onError_);
        onError_(std::move(result).status());
        return;
    }
    auto future = ectx()->getStorageClient()->addVertices(spaceId_,
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
