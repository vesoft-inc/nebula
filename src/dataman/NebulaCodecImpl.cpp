/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <string>
#include "base/Base.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "dataman/SchemaWriter.h"
#include "NebulaCodecImpl.h"

namespace nebula {
namespace dataman {

std::string NebulaCodecImpl::encode(std::vector<Value> values) {
  RowWriter writer(nullptr);
  for (auto&  value : values) {
    if (value.type() == typeid(int)) {
      writer <<  boost::any_cast<int>(value);
    } else if (value.type() == typeid(std::string)) {
      writer <<  boost::any_cast<std::string>(value);
    } else if (value.type() == typeid(double)) {
      writer <<  boost::any_cast<double>(value);
    } else if (value.type() == typeid(float)) {
      writer <<  boost::any_cast<float>(value);
    } else if (value.type() == typeid(bool)) {
      writer <<  boost::any_cast<bool>(value);
    }
  }
  std::string result = writer.encode();
  return result;
}

std::unordered_map<std::string, Value> NebulaCodecImpl::decode(std::string encoded,
        std::vector<std::pair<std::string, storage::cpp2::SupportedType>> fields) {
  auto schema = std::make_shared<SchemaWriter>();
  auto iter = fields.begin();
  for (; iter != fields.end(); iter++) {
    schema->appendCol(iter->first, iter->second);
  }

  folly::StringPiece piece;
  auto reader = RowReader::getRowReader(encoded, schema);
  std::unordered_map<std::string, Value> result;
  iter = fields.begin();
  for (; iter != fields.end(); iter++) {
    auto field = iter->first;
    switch (iter->second) {
      case storage::cpp2::SupportedType::BOOL:
        bool b;
        reader->getBool(field, b);
        result[field] = b;
        break;
      case storage::cpp2::SupportedType::INT:
        int32_t i;
        reader->getInt(field, i);
        result[field] = i;
        break;
      case storage::cpp2::SupportedType::STRING:
        // folly::StringPiece piece;
        reader->getString(field, piece);
        result[field] = boost::any(piece.toString());
        std::cout << field << " " << piece.toString() << std::endl;
        break;
      case storage::cpp2::SupportedType::VID:
        int64_t v;
        reader->getVid(field, v);
        result[field] = v;
        break;
      case storage::cpp2::SupportedType::FLOAT:
        float f;
        reader->getFloat(field, f);
        result[field] = f;
        break;
      case storage::cpp2::SupportedType::DOUBLE:
        double d;
        reader->getDouble(field, d);
        result[field] = d;
        break;
      case storage::cpp2::SupportedType::TIMESTAMP:
        break;
      case storage::cpp2::SupportedType::YEAR:
        break;
      case storage::cpp2::SupportedType::YEARMONTH:
        break;
      case storage::cpp2::SupportedType::DATE:
        break;
      case storage::cpp2::SupportedType::DATETIME:
        break;
      case storage::cpp2::SupportedType::PATH:
        break;
      default:
        // UNKNOWN ?
        break;
    }
  }
  return result;
}
}  // namespace dataman
}  // namespace nebula
