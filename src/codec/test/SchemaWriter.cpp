/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "codec/test/SchemaWriter.h"

#include "common/base/Base.h"

namespace nebula {

using meta::cpp2::Schema;
using nebula::cpp2::PropertyType;

SchemaWriter& SchemaWriter::appendCol(folly::StringPiece name,
                                      PropertyType type,
                                      int32_t fixedStrLen,
                                      bool nullable,
                                      Expression* defaultValue,
                                      meta::cpp2::GeoShape geoShape) noexcept {
  using folly::hash::SpookyHashV2;
  uint64_t hash = SpookyHashV2::Hash64(name.data(), name.size(), 0);
  DCHECK(nameIndex_.find(hash) == nameIndex_.end());

  int32_t offset = 0;
  if (columns_.size() > 0) {
    auto& prevCol = columns_.back();
    offset = prevCol.offset() + prevCol.size();
  } else {
    offset = 0;
  }

  int16_t size = 0;
  switch (type) {
    case PropertyType::BOOL:
      size = sizeof(bool);
      break;
    case PropertyType::INT8:
      size = sizeof(int8_t);
      break;
    case PropertyType::INT16:
      size = sizeof(int16_t);
      break;
    case PropertyType::INT32:
      size = sizeof(int32_t);
      break;
    case PropertyType::INT64:
      size = sizeof(int64_t);
      break;
    case PropertyType::VID:
      size = sizeof(int64_t);
      break;
    case PropertyType::FLOAT:
      size = sizeof(float);
      break;
    case PropertyType::DOUBLE:
      size = sizeof(double);
      break;
    case PropertyType::STRING:
      size = 2 * sizeof(int32_t);
      break;
    case PropertyType::FIXED_STRING:
      CHECK_GT(fixedStrLen, 0) << "Fixed string length has to be greater than 0";
      size = fixedStrLen;
      break;
    case PropertyType::TIMESTAMP:
      size = sizeof(Timestamp);
      break;
    case PropertyType::DATE:
      size = sizeof(int16_t) + 2 * sizeof(int8_t);
      break;
    case PropertyType::TIME:
      size = 3 * sizeof(int8_t) + sizeof(int32_t);
      break;
    case PropertyType::DATETIME:
      size = sizeof(int16_t) + 5 * sizeof(int8_t) + sizeof(int32_t);
      break;
    case PropertyType::GEOGRAPHY:
      size = 2 * sizeof(int32_t);  // as same as STRING
      break;
    case PropertyType::DURATION:
      size = sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
      break;
    default:
      LOG(FATAL) << "Unknown column type";
  }

  size_t nullFlagPos = 0;
  if (nullable) {
    nullFlagPos = numNullableFields_++;
  }

  columns_.emplace_back(name.toString(),
                        type,
                        size,
                        nullable,
                        offset,
                        nullFlagPos,
                        defaultValue ? defaultValue->encode() : "",
                        geoShape);
  nameIndex_.emplace(std::make_pair(hash, columns_.size() - 1));

  return *this;
}

}  // namespace nebula
