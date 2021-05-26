/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman/ResultSchemaProvider.h"

namespace nebula {

using folly::hash::SpookyHashV2;
using cpp2::ColumnDef;
using cpp2::ValueType;
using cpp2::Schema;

/***********************************
 *
 * ResultSchemaField
 *
 **********************************/
ResultSchemaProvider::ResultSchemaField::ResultSchemaField(const ColumnDef* col)
    : column_(col) {}


const char* ResultSchemaProvider::ResultSchemaField::getName() const {
    DCHECK(!!column_);
    return column_->get_name().c_str();
}


const ValueType& ResultSchemaProvider::ResultSchemaField::getType() const {
    DCHECK(!!column_);
    return column_->get_type();
}


bool ResultSchemaProvider::ResultSchemaField::isValid() const {
    return column_ != nullptr;
}


bool ResultSchemaProvider::ResultSchemaField::hasDefaultValue() const {
    if (!isValid() || !column_->__isset.default_value) {
        return false;
    }
    return true;
}


VariantType ResultSchemaProvider::ResultSchemaField::getDefaultValue() const {
    CHECK(column_ != nullptr);
    VariantType defaultValue;
    switch (column_->default_value.getType()) {
        case nebula::cpp2::Value::Type::__EMPTY__:
            LOG(FATAL) << "Empty type ";
        case nebula::cpp2::Value::Type::int_value:
            defaultValue = column_->default_value.get_int_value();
            break;
        case nebula::cpp2::Value::Type::bool_value:
            defaultValue = column_->default_value.get_bool_value();
            break;
        case nebula::cpp2::Value::Type::double_value:
            defaultValue = column_->default_value.get_double_value();
            break;
        case nebula::cpp2::Value::Type::string_value:
            defaultValue = column_->default_value.get_string_value();
            break;
        case nebula::cpp2::Value::Type::timestamp:
            defaultValue = column_->default_value.get_timestamp();
            break;
    }
    return defaultValue;
}


/***********************************
 *
 * ResultSchemaProvider
 *
 **********************************/
ResultSchemaProvider::ResultSchemaProvider(Schema schema)
        : columns_(std::move(schema.get_columns())) {
    for (int64_t i = 0; i < static_cast<int64_t>(columns_.size()); i++) {
        const std::string& name = columns_[i].get_name();
        nameIndex_.emplace(std::make_pair(SpookyHashV2::Hash64(name.data(), name.size(), 0), i));
    }
}


size_t ResultSchemaProvider::getNumFields() const noexcept {
    return columns_.size();
}


int64_t ResultSchemaProvider::getFieldIndex(const folly::StringPiece name) const {
    uint64_t hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
    auto iter = nameIndex_.find(hash);
    if (iter == nameIndex_.end()) {
        return -1;
    }
    return iter->second;
}


const char* ResultSchemaProvider::getFieldName(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return nullptr;
    }
    return columns_[index].get_name().c_str();
}


const ValueType& ResultSchemaProvider::getFieldType(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return CommonConstants::kInvalidValueType();
    }

    return columns_[index].get_type();
}


const ValueType& ResultSchemaProvider::getFieldType(const folly::StringPiece name) const {
    auto index = getFieldIndex(name);
    if (index < 0) {
        return CommonConstants::kInvalidValueType();
    }
    return columns_[index].get_type();
}


std::shared_ptr<const meta::SchemaProviderIf::Field> ResultSchemaProvider::field(
        int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return std::shared_ptr<ResultSchemaField>();
    }
    return std::make_shared<ResultSchemaField>(&(columns_[index]));
}


std::shared_ptr<const meta::SchemaProviderIf::Field> ResultSchemaProvider::field(
        const folly::StringPiece name) const {
    auto index = getFieldIndex(name);
    if (index < 0) {
        return std::shared_ptr<ResultSchemaField>();
    }
    return std::make_shared<ResultSchemaField>(&(columns_[index]));
}

nebula::cpp2::Schema ResultSchemaProvider::toSchema() const {
    nebula::cpp2::Schema schema;
    schema.set_columns(columns_);
    return schema;
}

const StatusOr<VariantType>
ResultSchemaProvider::getDefaultValue(const folly::StringPiece name) const {
    auto index = getFieldIndex(name);
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return Status::Error("Unknown field \"%s\"", name.toString().c_str());
    }
    return getDefaultValue(index);
}

const StatusOr<VariantType>
ResultSchemaProvider::getDefaultValue(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << columns_.size() << "]";
        return Status::Error("Index[%ld] is out of range[0-%zu]", index, columns_.size());
    }
    auto resultSchema = std::make_shared<ResultSchemaField>(&(columns_[index]));
    if (!resultSchema->hasDefaultValue()) {
        VLOG(2) << "Index " << index << " has no default value";
        return Status::Error("%ld has no default value", index);
    }
    return resultSchema->getDefaultValue();
}

}  // namespace nebula

