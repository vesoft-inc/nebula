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


bool ResultSchemaProvider::ResultSchemaField::hasDefault() const {
    LOG(FATAL) << "Not Supported";
}


std::string ResultSchemaProvider::ResultSchemaField::getDefaultValue() const {
    LOG(FATAL) << "Not Supported";
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

}  // namespace nebula

