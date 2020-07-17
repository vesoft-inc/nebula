/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "meta/NebulaSchemaProvider.h"

namespace nebula {
namespace meta {

SchemaVer NebulaSchemaProvider::getVersion() const noexcept {
    return ver_;
}

size_t NebulaSchemaProvider::getNumFields() const noexcept {
    return fields_.size();
}

int64_t NebulaSchemaProvider::getFieldIndex(const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        // Not found
        return -1;
    } else {
        return it->second;
    }
}

const char* NebulaSchemaProvider::getFieldName(int64_t index) const {
    if (UNLIKELY(index < 0) || UNLIKELY(index >= static_cast<int64_t>(fields_.size()))) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
        return nullptr;
    }

    return fields_[index]->getName();
}

const cpp2::ValueType& NebulaSchemaProvider::getFieldType(int64_t index) const {
    if (UNLIKELY(index < 0) || UNLIKELY(index >= static_cast<int64_t>(fields_.size()))) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
        return CommonConstants::kInvalidValueType();
    }

    return fields_[index]->getType();
}

const cpp2::ValueType& NebulaSchemaProvider::getFieldType(const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (UNLIKELY(fieldNameIndex_.end() == it)) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return CommonConstants::kInvalidValueType();
    }

    return fields_[it->second]->getType();
}

std::shared_ptr<const SchemaProviderIf::Field> NebulaSchemaProvider::field(
        int64_t index) const {
    if (index < 0) {
        VLOG(2) << "Invalid index " << index;
        return nullptr;
    }
    if (index >= static_cast<int64_t>(fields_.size())) {
        VLOG(2) << "Index " << index << " is out of range";
        return nullptr;
    }

    return fields_[index];
}

std::shared_ptr<const SchemaProviderIf::Field> NebulaSchemaProvider::field(
        const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return nullptr;
    }

    return fields_[it->second];
}

void NebulaSchemaProvider::addField(folly::StringPiece name,
                                    nebula::cpp2::ValueType&& type) {
    fields_.emplace_back(std::make_shared<SchemaField>(name.toString(),
                                                       std::move(type)));
    fieldNameIndex_.emplace(name.toString(),
                            static_cast<int64_t>(fields_.size() - 1));
}

void NebulaSchemaProvider::addDefaultValue(folly::StringPiece name,
                                           const nebula::cpp2::Value &value) {
    auto it = fieldNameIndex_.find(name.toString());
    if (UNLIKELY(fieldNameIndex_.end() == it)) {
        LOG(ERROR) << "Unknown field \"" << name.toString() << "\"";
        return;
    }
    VariantType defaultValue;
    switch (value.getType()) {
        case nebula::cpp2::Value::Type::__EMPTY__:
            VLOG(1) << "Empty field \"" << name.toString() << "\"";
            return;
        case nebula::cpp2::Value::Type::int_value:
            defaultValue = value.get_int_value();
            break;
        case nebula::cpp2::Value::Type::bool_value:
            defaultValue = value.get_bool_value();
            break;
        case nebula::cpp2::Value::Type::double_value:
            defaultValue = value.get_double_value();
            break;
        case nebula::cpp2::Value::Type::string_value:
            defaultValue = value.get_string_value();
            break;
        case nebula::cpp2::Value::Type::timestamp:
            defaultValue = value.get_timestamp();
            break;
    }
    fields_[it->second]->setDefaultValue(std::move(defaultValue));
}

void NebulaSchemaProvider::setProp(nebula::cpp2::SchemaProp schemaProp) {
    schemaProp_ = std::move(schemaProp);
}

const nebula::cpp2::SchemaProp NebulaSchemaProvider::getProp() const {
    return schemaProp_;
}

const StatusOr<VariantType>
NebulaSchemaProvider::getDefaultValue(const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (UNLIKELY(fieldNameIndex_.end() == it)) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return Status::Error("Unknown field \"%s\"", name.toString().c_str());
    }
    return getDefaultValue(it->second);
}

const StatusOr<VariantType>
NebulaSchemaProvider::getDefaultValue(int64_t index) const {
    if (UNLIKELY(index < 0) || UNLIKELY(index >= static_cast<int64_t>(fields_.size()))) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
        return Status::Error("Index[%ld] is out of range[0-%zu]", index, fields_.size());
    }
    if (!fields_[index]->hasDefaultValue()) {
        VLOG(2) << "Index " << index << " without default value";
        return Status::Error("Index %ld without default value", index);
    }
    return fields_[index]->getDefaultValue();
}

nebula::cpp2::Schema NebulaSchemaProvider::toSchema() const {
    nebula::cpp2::Schema schema;
    std::vector<nebula::cpp2::ColumnDef> columns;
    for (auto& field : fields_) {
        nebula::cpp2::ColumnDef column;
        column.set_name(field->getName());
        column.set_type(field->getType());
        columns.emplace_back(column);
    }
    schema.set_columns(std::move(columns));
    schema.set_schema_prop(schemaProp_);
    return schema;
}

}  // namespace meta
}  // namespace nebula

