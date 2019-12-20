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
            LOG(ERROR) << "No schema found for " << tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        auto tagId = tagStatus.value();
        auto schema = ectx()->schemaManager()->getTagSchema(spaceId_, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        auto props = item->properties();
        if (props.size() > schema->getNumFields()) {
            LOG(ERROR) << "Input props number " << props.size()
                       << ", schema fields number " << schema->getNumFields();
            return Status::Error("Wrong number of props");
        }

        auto *mc = ectx()->getMetaClient();

        std::unordered_map<std::string, int32_t> propsPosition;
        for (size_t i = 0; i < schema->getNumFields(); i++) {
            std::string name = schema->getFieldName(i);
            auto it = std::find_if(props.begin(), props.end(),
                                   [name](std::string *prop) { return *prop == name;});

            // If the property name not find in schema's field
            // We need to check the default value and save it.
            if (it == props.end()) {
                auto valueResult = mc->getTagDefaultValue(spaceId_, tagId, name).get();
                if (!valueResult.ok()) {
                    LOG(ERROR) << "Not exist default value: " << name;
                    return Status::Error("Not exist default value");
                } else {
                    VLOG(3) << "Default Value: " << name << ":" << valueResult.value();
                    defaultValues_.emplace(name, valueResult.value());
                }
            } else {
                int index = std::distance(props.begin(), it);
                propsPosition.emplace(name, index);
            }
        }

        tagIds_.emplace_back(tagId);
        schemas_.emplace_back(std::move(schema));
        tagProps_.emplace_back(std::move(props));
        propsPositions_.emplace_back(std::move(propsPosition));
    }
    return Status::OK();
}

StatusOr<std::vector<storage::cpp2::Vertex>> InsertVertexExecutor::prepareVertices() {
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);

    std::vector<storage::cpp2::Vertex> vertices(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
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

        std::vector<storage::cpp2::Tag> tags(tagIds_.size());

        int32_t valuePosition = 0;
        for (auto index = 0u; index < tagIds_.size(); index++) {
            auto &tag = tags[index];
            auto tagId = tagIds_[index];
            auto props = tagProps_[index];
            auto schema = schemas_[index];
            auto propsPosition = propsPositions_[index];

            RowWriter writer(schema);
            VariantType value;
            auto schemaNumFields = schema->getNumFields();
            for (size_t schemaIndex = 0; schemaIndex < schemaNumFields; schemaIndex++) {
                auto fieldName = schema->getFieldName(schemaIndex);
                auto positionIter = propsPosition.find(fieldName);

                auto schemaType = schema->getFieldType(schemaIndex);
                if (positionIter != propsPosition.end()) {
                    auto position = propsPosition[fieldName];
                    value = values[position + valuePosition];

                    if (!checkValueType(schemaType, value)) {
                       LOG(ERROR) << "ValueType is wrong, schema type "
                                   << static_cast<int32_t>(schemaType.type)
                                   << ", input type " <<  value.which();
                        return Status::Error("ValueType is wrong");
                    }
                } else {
                    // fetch default value from cache
                    auto result = transformDefaultValue(schemaType.type, defaultValues_[fieldName]);
                    if (!result.ok()) {
                        return result.status();
                    }

                    value = result.value();
                    VLOG(3) << "Supplement default value : " << fieldName << " : " << value;
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
            }

            tag.set_tag_id(tagId);
            tag.set_props(writer.encode());
            valuePosition += propsPosition.size();
        }

        auto& vertex = vertices[i];
        vertex.set_id(id);
        vertex.set_tags(std::move(tags));
    }

    return vertices;
}


void InsertVertexExecutor::execute() {
    auto status = check();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getInsertVertexStats());
        return;
    }

    auto result = prepareVertices();
    if (!result.ok()) {
        LOG(ERROR) << "Insert vertices failed, error " << result.status().toString();
        doError(result.status(), ectx()->getGraphStats()->getInsertVertexStats());
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
            const auto& failedCodes = resp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << "Insert vertices failed, error " << static_cast<int32_t>(it->second)
                           << ", part " << it->first;
            }
            doError(Status::Error("Internal Error"),
                    ectx()->getGraphStats()->getInsertVertexStats());
            return;
        }
        doFinish(Executor::ProcessControl::kNext,
                 ectx()->getGraphStats()->getInsertVertexStats(),
                 rows_.size());
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal Error"),
                ectx()->getGraphStats()->getInsertVertexStats());
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula


