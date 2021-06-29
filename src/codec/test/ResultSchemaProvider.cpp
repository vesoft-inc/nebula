/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/test/ResultSchemaProvider.h"

namespace nebula {

using folly::hash::SpookyHashV2;
using meta::cpp2::PropertyType;
using meta::cpp2::Schema;

/***********************************
 *
 * ResultSchemaField
 *
 **********************************/
ResultSchemaProvider::ResultSchemaField::ResultSchemaField(
        std::string name,
        meta::cpp2::PropertyType type,
        int16_t size,
        bool nullable,
        int32_t offset,
        size_t nullFlagPos,
        Expression* defaultValue)
    : name_(std::move(name))
    , type_(type)
    , size_(size)
    , nullable_(nullable)
    , offset_(offset)
    , nullFlagPos_(nullFlagPos)
    , defaultValue_(defaultValue) {}


const char* ResultSchemaProvider::ResultSchemaField::name() const {
    return name_.c_str();
}


PropertyType ResultSchemaProvider::ResultSchemaField::type() const {
    return type_;
}


bool ResultSchemaProvider::ResultSchemaField::hasDefault() const {
    return defaultValue_ != nullptr;
}


bool ResultSchemaProvider::ResultSchemaField::nullable() const {
    return nullable_;
}


Expression* ResultSchemaProvider::ResultSchemaField::defaultValue() const {
    return defaultValue_;
}


size_t ResultSchemaProvider::ResultSchemaField::size() const {
    return size_;
}


size_t ResultSchemaProvider::ResultSchemaField::offset() const {
    return offset_;
}


size_t ResultSchemaProvider::ResultSchemaField::nullFlagPos() const {
    return nullFlagPos_;
}


/***********************************
 *
 * ResultSchemaProvider
 *
 **********************************/
size_t ResultSchemaProvider::getNumFields() const noexcept {
    return columns_.size();
}


size_t ResultSchemaProvider::getNumNullableFields() const noexcept {
    return numNullableFields_;
}


size_t ResultSchemaProvider::size() const noexcept {
    if (columns_.size() > 0) {
        auto& last = columns_.back();
        return last.offset() + last.size();
    } else {
        return 0;
    }
}


int64_t ResultSchemaProvider::getFieldIndex(const std::string& name) const {
    uint64_t hash = SpookyHashV2::Hash64(name.data(), name.size(), 0);
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
    return columns_[index].name();
}


PropertyType ResultSchemaProvider::getFieldType(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return PropertyType::UNKNOWN;
    }

    return columns_[index].type();
}


PropertyType ResultSchemaProvider::getFieldType(const std::string& name) const {
    auto index = getFieldIndex(name);
    if (index < 0) {
        return PropertyType::UNKNOWN;
    }
    return columns_[index].type();
}


const meta::SchemaProviderIf::Field* ResultSchemaProvider::field(int64_t index)
        const {
    if (index < 0 || index >= static_cast<int64_t>(columns_.size())) {
        return nullptr;
    }
    return &(columns_[index]);
}


const meta::SchemaProviderIf::Field* ResultSchemaProvider::field(
        const std::string& name) const {
    auto index = getFieldIndex(name);
    if (index < 0) {
        return nullptr;
    }
    return &(columns_[index]);
}

}  // namespace nebula

