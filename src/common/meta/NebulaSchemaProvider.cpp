/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/meta/NebulaSchemaProvider.h"

namespace nebula {
namespace meta {

SchemaVer NebulaSchemaProvider::getVersion() const noexcept {
    return ver_;
}


size_t NebulaSchemaProvider::getNumFields() const noexcept {
    return fields_.size();
}


size_t NebulaSchemaProvider::getNumNullableFields() const noexcept {
    return numNullableFields_;
}


size_t NebulaSchemaProvider::size() const noexcept {
    if (fields_.size() > 0) {
        auto& lastField = fields_.back();
        return lastField.offset() + lastField.size();
    }

    return 0;
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

    return fields_[index].name();
}


cpp2::PropertyType NebulaSchemaProvider::getFieldType(int64_t index) const {
    if (UNLIKELY(index < 0) || UNLIKELY(index >= static_cast<int64_t>(fields_.size()))) {
        LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
        return cpp2::PropertyType::UNKNOWN;
    }

    return fields_[index].type();
}


cpp2::PropertyType NebulaSchemaProvider::getFieldType(const folly::StringPiece name)
        const {
    auto it = fieldNameIndex_.find(name.toString());
    if (UNLIKELY(fieldNameIndex_.end() == it)) {
        LOG(ERROR) << "Unknown field \"" << name.toString() << "\"";
        return cpp2::PropertyType::UNKNOWN;
    }

    return fields_[it->second].type();
}


const SchemaProviderIf::Field* NebulaSchemaProvider::field(int64_t index) const {
    if (index < 0) {
        VLOG(2) << "Invalid index " << index;
        return nullptr;
    }
    if (index >= static_cast<int64_t>(fields_.size())) {
        VLOG(2) << "Index " << index << " is out of range";
        return nullptr;
    }

    return &fields_[index];
}


const SchemaProviderIf::Field* NebulaSchemaProvider::field(
        const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return nullptr;
    }

    return &fields_[it->second];
}


void NebulaSchemaProvider::addField(folly::StringPiece name,
                                    cpp2::PropertyType type,
                                    size_t fixedStrLen,
                                    bool nullable,
                                    Expression* defaultValue) {
    size_t size = 0;
    switch (type) {
        case cpp2::PropertyType::BOOL:
            size = 1;
            break;
        case cpp2::PropertyType::INT64:
            size = sizeof(int64_t);
            break;
        case cpp2::PropertyType::INT32:
            size = sizeof(int32_t);
            break;
        case cpp2::PropertyType::INT16:
            size = sizeof(int16_t);
            break;
        case cpp2::PropertyType::INT8:
            size = sizeof(int8_t);
            break;
        case cpp2::PropertyType::VID:
            // VID is deprecated in V2
            size = sizeof(int64_t);
            break;
        case cpp2::PropertyType::FLOAT:
            size = sizeof(float);
            break;
        case cpp2::PropertyType::DOUBLE:
            size = sizeof(double);
            break;
        case cpp2::PropertyType::STRING:
            size = 8;  // string offset + string length
            break;
        case cpp2::PropertyType::FIXED_STRING:
            CHECK_GT(fixedStrLen, 0)
                << "Fixed string length must be greater than zero";
            size = fixedStrLen;
            break;
        case cpp2::PropertyType::TIMESTAMP:
            size = sizeof(int64_t);
            break;
        case cpp2::PropertyType::DATE:
            size = sizeof(int16_t) +    // year
                   sizeof(int8_t) +     // month
                   sizeof(int8_t);      // day
            break;
        case cpp2::PropertyType::DATETIME:
            size = sizeof(int16_t) +    // year
                   sizeof(int8_t) +     // month
                   sizeof(int8_t) +     // day
                   sizeof(int8_t) +     // hour
                   sizeof(int8_t) +     // minute
                   sizeof(int8_t) +     // sec
                   sizeof(int32_t);     // microsec
            break;
        default:
            LOG(FATAL) << "Incorrect field type";
    }

    size_t offset = 0;
    if (fields_.size() > 0) {
        auto& lastField = fields_.back();
        offset = lastField.offset() + lastField.size();
    }

    size_t nullFlagPos = 0;
    if (nullable) {
        nullFlagPos = numNullableFields_++;
    }

    fields_.emplace_back(name.toString(),
                         type,
                         nullable,
                         defaultValue != nullptr,
                         defaultValue,
                         size,
                         offset,
                         nullFlagPos);
    fieldNameIndex_.emplace(name.toString(),
                            static_cast<int64_t>(fields_.size() - 1));
}


void NebulaSchemaProvider::setProp(cpp2::SchemaProp schemaProp) {
    schemaProp_ = std::move(schemaProp);
}


const cpp2::SchemaProp NebulaSchemaProvider::getProp() const {
    return schemaProp_;
}


StatusOr<std::pair<std::string, int64_t>> NebulaSchemaProvider::getTTLInfo() const {
    if (!schemaProp_.__isset.ttl_col) {
        return Status::Error("TTL not set");
    }
    std::string ttlCol = *schemaProp_.get_ttl_col();
    int64_t ttlDuration = schemaProp_.__isset.ttl_duration ?
                          *schemaProp_.get_ttl_duration() :
                          0;
    // Only support the specified ttl_col mode
    // Not specifying or non-positive ttl_duration behaves like ttl_duration = infinity
    if (ttlCol.empty() || ttlDuration <= 0) {
        return Status::Error("TTL property is invalid");
    }
    return std::make_pair(ttlCol, ttlDuration);
}

}  // namespace meta
}  // namespace nebula
