/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "codec/test/SchemaWriter.h"

namespace nebula {

using meta::cpp2::Schema;
using meta::cpp2::PropertyType;


SchemaWriter& SchemaWriter::appendCol(folly::StringPiece name,
                                      PropertyType type,
                                      int32_t fixedStrLen,
                                      bool nullable,
                                      Expression* defaultValue) noexcept {
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
                          defaultValue);
    nameIndex_.emplace(std::make_pair(hash, columns_.size() - 1));

    return *this;
}

}  // namespace nebula

