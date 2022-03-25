/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/meta/NebulaSchemaProvider.h"

#include "common/base/Base.h"

namespace nebula {
namespace meta {

using nebula::cpp2::PropertyType;

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

int64_t NebulaSchemaProvider::getFieldIndex(const std::string& name) const {
  auto it = fieldNameIndex_.find(name);
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

PropertyType NebulaSchemaProvider::getFieldType(int64_t index) const {
  if (UNLIKELY(index < 0) || UNLIKELY(index >= static_cast<int64_t>(fields_.size()))) {
    LOG(ERROR) << "Index[" << index << "] is out of range[0-" << fields_.size() << "]";
    return PropertyType::UNKNOWN;
  }

  return fields_[index].type();
}

PropertyType NebulaSchemaProvider::getFieldType(const std::string& name) const {
  auto it = fieldNameIndex_.find(name);
  if (UNLIKELY(fieldNameIndex_.end() == it)) {
    LOG(ERROR) << "Unknown field \"" << name << "\"";
    return PropertyType::UNKNOWN;
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

const SchemaProviderIf::Field* NebulaSchemaProvider::field(const std::string& name) const {
  auto it = fieldNameIndex_.find(name);
  if (it == fieldNameIndex_.end()) {
    VLOG(2) << "Unknown field \"" << name << "\"";
    return nullptr;
  }

  return &fields_[it->second];
}

void NebulaSchemaProvider::addField(folly::StringPiece name,
                                    PropertyType type,
                                    size_t fixedStrLen,
                                    bool nullable,
                                    std::string defaultValue,
                                    cpp2::GeoShape geoShape) {
  size_t size = fieldSize(type, fixedStrLen);

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
                       defaultValue != "",
                       defaultValue,
                       size,
                       offset,
                       nullFlagPos,
                       geoShape);
  fieldNameIndex_.emplace(name.toString(), static_cast<int64_t>(fields_.size() - 1));
}

/*static*/
std::size_t NebulaSchemaProvider::fieldSize(PropertyType type, std::size_t fixedStrLimit) {
  switch (type) {
    case PropertyType::BOOL:
      return 1;
    case PropertyType::INT64:
      return sizeof(int64_t);
    case PropertyType::INT32:
      return sizeof(int32_t);
    case PropertyType::INT16:
      return sizeof(int16_t);
    case PropertyType::INT8:
      return sizeof(int8_t);
    case PropertyType::VID:
      // VID is deprecated in V2
      return sizeof(int64_t);
    case PropertyType::FLOAT:
      return sizeof(float);
    case PropertyType::DOUBLE:
      return sizeof(double);
    case PropertyType::STRING:
      return 8;  // string offset + string length
    case PropertyType::FIXED_STRING:
      CHECK_GT(fixedStrLimit, 0) << "Fixed string length must be greater than zero";
      return fixedStrLimit;
    case PropertyType::TIMESTAMP:
      return sizeof(int64_t);
    case PropertyType::DATE:
      return sizeof(int16_t) +  // year
             sizeof(int8_t) +   // month
             sizeof(int8_t);    // day
    case PropertyType::TIME:
      return sizeof(int8_t) +  // hour
             sizeof(int8_t) +  // minute
             sizeof(int8_t) +  // sec
             sizeof(int32_t);  // microsec
    case PropertyType::DATETIME:
      return sizeof(int16_t) +  // year
             sizeof(int8_t) +   // month
             sizeof(int8_t) +   // day
             sizeof(int8_t) +   // hour
             sizeof(int8_t) +   // minute
             sizeof(int8_t) +   // sec
             sizeof(int32_t);   // microsec
    case PropertyType::GEOGRAPHY:
      return 8;  // as same as STRING
    case PropertyType::DURATION:
      return sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
    case PropertyType::UNKNOWN:
      break;
  }
  LOG(FATAL) << "Incorrect field type " << static_cast<int>(type);
}

void NebulaSchemaProvider::setProp(cpp2::SchemaProp schemaProp) {
  schemaProp_ = std::move(schemaProp);
}

const cpp2::SchemaProp NebulaSchemaProvider::getProp() const {
  return schemaProp_;
}

StatusOr<std::pair<std::string, int64_t>> NebulaSchemaProvider::getTTLInfo() const {
  if (!schemaProp_.ttl_col_ref().has_value()) {
    return Status::Error("TTL not set");
  }
  std::string ttlCol = *schemaProp_.get_ttl_col();
  int64_t ttlDuration =
      schemaProp_.ttl_duration_ref().has_value() ? *schemaProp_.get_ttl_duration() : 0;
  // Only support the specified ttl_col mode
  // Not specifying or non-positive ttl_duration behaves like ttl_duration =
  // infinity
  if (ttlCol.empty() || ttlDuration <= 0) {
    return Status::Error("TTL property is invalid");
  }
  return std::make_pair(ttlCol, ttlDuration);
}

}  // namespace meta
}  // namespace nebula
