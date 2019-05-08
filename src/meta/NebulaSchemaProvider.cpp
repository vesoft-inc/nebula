/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    CHECK_GE(index, 0) << "Invalid index " << index;
    CHECK_LT(index, fields_.size()) << "Index is out of range";

    return fields_[index]->getName();
}

const cpp2::ValueType& NebulaSchemaProvider::getFieldType(int64_t index) const {
    CHECK_GE(index, 0) << "Invalid index " << index;
    CHECK_LT(index, fields_.size()) << "Index is out of range";

    return fields_[index]->getType();
}

const cpp2::ValueType& NebulaSchemaProvider::getFieldType(const folly::StringPiece name)
        const {
    auto it = fieldNameIndex_.find(name.toString());
    CHECK(it != fieldNameIndex_.end())
        << "Unknown field \"" << name.toString() << "\"";
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

}  // namespace meta
}  // namespace nebula

