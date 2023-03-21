/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "codec/NebulaCodecImpl.h"

#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"
#include "common/base/Base.h"

/**
 * Report error message
 */
#define PRINT_ERROR_MESSAGE(code, value)                                                   \
  if (ResultType::SUCCEEDED == code) {                                                     \
    result[field] = value;                                                                 \
  } else {                                                                                 \
    LOG(ERROR) << "ResultType : " << apache::thrift::util::enumNameSafe(code) << " Value " \
               << value << std::endl;                                                      \
  }

namespace nebula {

std::string NebulaCodecImpl::encode(std::vector<Value> values,
                                    std::shared_ptr<const meta::NebulaSchemaProvider> schema) {
  RowWriter writer(schema);
  for (auto& value : values) {
    if (value.type() == typeid(int32_t)) {
      writer << std::any_cast<int32_t>(value);
    } else if (value.type() == typeid(int64_t)) {
      writer << std::any_cast<int64_t>(value);
    } else if (value.type() == typeid(std::string)) {
      writer << std::any_cast<std::string>(value);
    } else if (value.type() == typeid(double)) {
      writer << std::any_cast<double>(value);
    } else if (value.type() == typeid(float)) {
      writer << std::any_cast<float>(value);
    } else if (value.type() == typeid(bool)) {
      writer << std::any_cast<bool>(value);
    } else {
      LOG(ERROR) << "Value Type :" << value.type().name() << std::endl;
    }
  }
  std::string result = writer.encode();
  return result;
}

StatusOr<std::unordered_map<std::string, Value>> NebulaCodecImpl::decode(
    std::string encoded, std::shared_ptr<const meta::NebulaSchemaProvider> schema) {
  if (encoded.empty()) {
    return Status::Error("encoded string is empty");
  }
  if (!schema) {
    return Status::Error("schema is not set");
  }

  folly::StringPiece piece;
  ResultType code;
  auto reader = RowReaderWrapper::getRowReader(encoded, schema);
  std::unordered_map<std::string, Value> result;
  for (size_t index = 0; index < schema->getNumFields(); index++) {
    auto field = schema->getFieldName(index);
    if (UNLIKELY(nullptr == field)) {
      return Status::Error("invalid field index");
    }
    switch (schema->getFieldType(index).get_type()) {
      case cpp2::SupportedType::BOOL:
        bool b;
        code = reader->getBool(field, b);
        PRINT_ERROR_MESSAGE(code, b);
        break;
      case cpp2::SupportedType::INT:
        int32_t i;
        code = reader->getInt(field, i);
        PRINT_ERROR_MESSAGE(code, i);
        break;
      case cpp2::SupportedType::STRING:
        code = reader->getString(field, piece);
        PRINT_ERROR_MESSAGE(code, piece.toString());
        break;
      case cpp2::SupportedType::VID:
        int64_t v;
        code = reader->getVid(field, v);
        PRINT_ERROR_MESSAGE(code, v);
        break;
      case cpp2::SupportedType::FLOAT:
        float f;
        code = reader->getFloat(field, f);
        PRINT_ERROR_MESSAGE(code, f);
        break;
      case cpp2::SupportedType::DOUBLE:
        double d;
        code = reader->getDouble(field, d);
        PRINT_ERROR_MESSAGE(code, d)
        break;
      case cpp2::SupportedType::TIMESTAMP:
        // TODO(darion) Support TIMESTAMP
        break;
      case cpp2::SupportedType::YEAR:
        // TODO(darion) Support YEAR
        break;
      case cpp2::SupportedType::YEARMONTH:
        // TODO(darion) Support YEARMONTH
        break;
      case cpp2::SupportedType::DATE:
        // TODO(darion) Support DATE
        break;
      case cpp2::SupportedType::DATETIME:
        // TODO(darion) Support DATETIME
        break;
      case cpp2::SupportedType::PATH:
        // TODO(darion) Support PATH
        break;
      default:
        // UNKNOWN
        break;
    }
  }
  return result;
}

}  // namespace nebula
