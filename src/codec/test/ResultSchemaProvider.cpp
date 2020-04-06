/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "codec/test/ResultSchemaProvider.h"

namespace nebula {

using folly::hash::SpookyHashV2;
using meta::cpp2::ColumnDef;
using meta::cpp2::PropertyType;
using meta::cpp2::Schema;

/***********************************
 *
 * ResultSchemaField
 *
 **********************************/
ResultSchemaProvider::ResultSchemaField::ResultSchemaField(const ColumnDef* col)
    : column_(col) {}


const char* ResultSchemaProvider::ResultSchemaField::name() const {
    DCHECK(!!column_);
    return column_->get_name().c_str();
}


PropertyType ResultSchemaProvider::ResultSchemaField::type() const {
    DCHECK(!!column_);
    return column_->get_type();
}


bool ResultSchemaProvider::ResultSchemaField::nullable() const {
    LOG(FATAL) << "Not Supported";
}


bool ResultSchemaProvider::ResultSchemaField::hasDefault() const {
    LOG(FATAL) << "Not Supported";
}


const Value& ResultSchemaProvider::ResultSchemaField::defaultValue() const {
    LOG(FATAL) << "Not Supported";
}

size_t ResultSchemaProvider::ResultSchemaField::size() const {
    return 0;
}

size_t ResultSchemaProvider::ResultSchemaField::offset() const {
    return 0;
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
        nameIndex_.emplace(
            std::make_pair(SpookyHashV2::Hash64(name.data(), name.size(), 0), i));
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


PropertyType ResultSchemaProvider::getFieldType(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return PropertyType::UNKNOWN;
    }

    return columns_[index].get_type();
}


PropertyType ResultSchemaProvider::getFieldType(const folly::StringPiece name) const {
    auto index = getFieldIndex(name);
    if (index < 0) {
        return PropertyType::UNKNOWN;
    }
    return columns_[index].get_type();
}


const meta::SchemaProviderIf::Field* ResultSchemaProvider::field(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return nullptr;
    }
    field_.reset(new ResultSchemaField(&(columns_[index])));
    return field_.get();
}


const meta::SchemaProviderIf::Field* ResultSchemaProvider::field(
        const folly::StringPiece name) const {
    auto index = getFieldIndex(name);
    if (index < 0) {
        return nullptr;
    }
    field_.reset(new ResultSchemaField(&(columns_[index])));
    return field_.get();
}

}  // namespace nebula

