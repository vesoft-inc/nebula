/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datamanlite/NebulaSchemaProvider.h"
#include "base/Logging.h"

namespace nebula {
namespace dataman {
namespace codec {

ValueType kUnknown = ValueType::UNKNOWN;

SchemaVer NebulaSchemaProvider::getVersion() const noexcept {
    return ver_;
}

size_t NebulaSchemaProvider::getNumFields() const noexcept {
    return fields_.size();
}

int64_t NebulaSchemaProvider::getFieldIndex(const Slice& name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        // Not found
        return -1;
    } else {
        return it->second;
    }
}

const char* NebulaSchemaProvider::getFieldName(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(fields_.size())) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
        return nullptr;
    }

    return fields_[index]->getName();
}

const ValueType& NebulaSchemaProvider::getFieldType(int64_t index) const {
    if (index < 0 || index >= static_cast<int64_t>(fields_.size())) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
        return kUnknown;
    }

    return fields_[index]->getType();
}

const ValueType& NebulaSchemaProvider::getFieldType(const Slice& name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (fieldNameIndex_.end() == it) {
        LOG(ERROR) << "Unknown field \"" << name.toString() << "\"";
        return kUnknown;
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
        const Slice& name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return nullptr;
    }

    return fields_[it->second];
}

void NebulaSchemaProvider::addField(const Slice& name,
                                    ValueType type) {
    fields_.emplace_back(std::make_shared<SchemaField>(name.toString(),
                                                       std::move(type)));
    fieldNameIndex_.emplace(name.toString(),
                            static_cast<int64_t>(fields_.size() - 1));
}


}  // namespace codec
}  // namespace dataman
}  // namespace nebula

