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
        LOG(ERROR) << "Unknown field \"" << name.toString() << "\"";
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

void NebulaSchemaProvider::setProp(nebula::cpp2::SchemaProp schemaProp) {
    schemaProp_ = std::move(schemaProp);
}

const nebula::cpp2::SchemaProp NebulaSchemaProvider::getProp() const {
    return schemaProp_;
}

}  // namespace meta
}  // namespace nebula

