/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <string>
#include <stdexcept>
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

StatusOr<std::unordered_map<std::string, Value>>
NebulaCodecImpl::decode(std::string encoded,
                        std::vector<std::pair<std::string, cpp2::SupportedType>> fields) {
    if (encoded.empty()) {
        return Status::Error("encoded string is empty");
    }
    if (fields.size() == 0) {
        return Status::Error("fields is not set");
    }

    auto schema = std::make_shared<SchemaWriter>();
    auto iter = fields.begin();
    for (; iter != fields.end(); iter++) {
        schema->appendCol(iter->first, iter->second);
    }

    folly::StringPiece piece;
    ResultType code;
    auto reader = RowReader::getRowReader(encoded, schema);
    std::unordered_map<std::string, Value> result;
    for (iter = fields.begin(); iter != fields.end(); iter++) {
        auto field = iter->first;
        switch (iter->second) {
            case cpp2::SupportedType::BOOL:
                bool b;
                code = reader->getBool(field, b);
                if (ResultType::SUCCEEDED == code) {
                    result[field] = b;
                }
                break;
            case cpp2::SupportedType::INT:
                int32_t i;
                code = reader->getInt(field, i);
                if (ResultType::SUCCEEDED == code) {
                    result[field] = i;
                }
                break;
            case cpp2::SupportedType::STRING:
                code = reader->getString(field, piece);
                if (ResultType::SUCCEEDED == code) {
                    result[field] = boost::any(piece.toString());
                }
                break;
            case cpp2::SupportedType::VID:
                int64_t v;
                code = reader->getVid(field, v);
                if (ResultType::SUCCEEDED == code) {
                    result[field] = v;
                }
                break;
            case cpp2::SupportedType::FLOAT:
                float f;
                code = reader->getFloat(field, f);
                if (ResultType::SUCCEEDED == code) {
                    result[field] = f;
                }
                break;
            case cpp2::SupportedType::DOUBLE:
                double d;
                code = reader->getDouble(field, d);
                if (ResultType::SUCCEEDED == code) {
                    result[field] = d;
                }
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
}  // namespace dataman
}  // namespace nebula
