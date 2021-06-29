/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/TimeUtils.h"
#include "common/time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "utils/DefaultValueContext.h"

namespace nebula {

RowWriterV2::RowWriterV2(const meta::SchemaProviderIf* schema)
        : schema_(schema)
        , numNullBytes_(0)
        , approxStrLen_(0)
        , finished_(false)
        , outOfSpaceStr_(false) {
    CHECK(!!schema_);

    // Reserve space for the header, the data, and the string values
    buf_.reserve(schema_->size() + schema_->getNumFields() / 8 + 8 + 1024);

    char header = 0;

    // Header and schema version
    //
    // The maximum number of bytes for the header and the schema version is 8
    //
    // The first byte is the header (os signature), it has the fourth-bit (from
    // the right side) set to one (0x08), and the right three bits indicate
    // the number of bytes used for the schema version.
    //
    // If all three bits are zero, the schema version is zero. If the number
    // of schema version bytes is one, the maximum schema version that can be
    // represented is 255 (0xFF). If the number of schema version is two, the
    // maximum schema version could be 65535 (0xFFFF), and so on.
    //
    // The maximum schema version we support is 0x00FFFFFFFFFFFFFF (7 bytes)
    int64_t ver = schema_->getVersion();
    if (ver > 0) {
        if (ver <= 0x00FF) {
            header = 0x09;  // 0x08 | 0x01, one byte for the schema version
            headerLen_ = 2;
        } else if (ver < 0x00FFFF) {
            header = 0x0A;  // 0x08 | 0x02, two bytes for the schema version
            headerLen_ = 3;
        } else if (ver < 0x00FFFFFF) {
            header = 0x0B;  // 0x08 | 0x03, three bytes for the schema version
            headerLen_ = 4;
        } else if (ver < 0x00FFFFFFFF) {
            header = 0x0C;  // 0x08 | 0x04, four bytes for the schema version
            headerLen_ = 5;
        } else if (ver < 0x00FFFFFFFFFF) {
            header = 0x0D;  // 0x08 | 0x05, five bytes for the schema version
            headerLen_ = 6;
        } else if (ver < 0x00FFFFFFFFFFFF) {
            header = 0x0E;  // 0x08 | 0x06, six bytes for the schema version
            headerLen_ = 7;
        } else if (ver < 0x00FFFFFFFFFFFFFF) {
            header = 0x0F;  // 0x08 | 0x07, severn bytes for the schema version
            headerLen_ = 8;
        } else {
            LOG(FATAL) << "Schema version too big";
        }
        buf_.append(&header, 1);
        buf_.append(reinterpret_cast<char*>(&ver), buf_[0] & 0x07);
    } else {
        header = 0x08;
        headerLen_ = 1;
        buf_.append(&header, 1);
    }

    // Null flags
    size_t numNullables = schema_->getNumNullableFields();
    if (numNullables > 0) {
        numNullBytes_ = ((numNullables - 1) >> 3) + 1;
    }

    // Reserve the space for the data, including the Null bits
    // All variant length string will be appended to the end
    buf_.resize(headerLen_ + numNullBytes_ + schema_->size(), '\0');

    isSet_.resize(schema_->getNumFields(), false);
}


RowWriterV2::RowWriterV2(const meta::SchemaProviderIf* schema, std::string&& encoded)
        : schema_(schema)
        , finished_(false)
        , outOfSpaceStr_(false) {
    auto len = encoded.size();
    buf_ = std::move(encoded).substr(0, len - sizeof(int64_t));
    processV2EncodedStr();
}


RowWriterV2::RowWriterV2(const meta::SchemaProviderIf* schema, const std::string& encoded)
        : schema_(schema)
        , buf_(encoded.substr(0, encoded.size() - sizeof(int64_t)))
        , finished_(false)
        , outOfSpaceStr_(false) {
    processV2EncodedStr();
}


RowWriterV2::RowWriterV2(RowReader& reader)
        : RowWriterV2(reader.getSchema()) {
    for (size_t i = 0; i < reader.numFields(); i++) {
        Value v = reader.getValueByIndex(i);
        switch (v.type()) {
            case Value::Type::NULLVALUE:
                setNull(i);
                break;
            case Value::Type::BOOL:
                set(i, v.getBool());
                break;
            case Value::Type::INT:
                set(i, v.getInt());
                break;
            case Value::Type::FLOAT:
                set(i, v.getFloat());
                break;
            case Value::Type::STRING:
                approxStrLen_ += v.getStr().size();
                set(i, v.moveStr());
                break;
            case Value::Type::DATE:
                set(i, v.moveDate());
                break;
            case Value::Type::TIME:
                set(i, v.moveTime());
                break;
            case Value::Type::DATETIME:
                set(i, v.moveDateTime());
                break;
            default:
                LOG(FATAL) << "Invalid data: " << v << ", type: " << v.typeName();
        }
        isSet_[i] = true;
    }
}


void RowWriterV2::processV2EncodedStr() noexcept {
    CHECK_EQ(0x08, buf_[0] & 0x18);
    int32_t verBytes = buf_[0] & 0x07;
    SchemaVer ver = 0;
    if (verBytes > 0) {
        memcpy(reinterpret_cast<void*>(&ver), &buf_[1], verBytes);
    }
    CHECK_EQ(ver, schema_->getVersion())
        << "The data is encoded by schema version " << ver
        << ", while the provided schema version is " << schema_->getVersion();

    headerLen_ = verBytes + 1;

    // Null flags
    size_t numNullables = schema_->getNumNullableFields();
    if (numNullables > 0) {
        numNullBytes_ = ((numNullables - 1) >> 3) + 1;
    } else {
        numNullBytes_ = 0;
    }

    approxStrLen_ = buf_.size() - headerLen_ - numNullBytes_ - schema_->size() - sizeof(int64_t);
    isSet_.resize(schema_->getNumFields(), true);
}


void RowWriterV2::setNullBit(ssize_t pos) noexcept {
    static const uint8_t orBits[] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

    size_t offset = headerLen_ + (pos >> 3);
    buf_[offset] = buf_[offset] | orBits[pos & 0x0000000000000007L];
}


void RowWriterV2::clearNullBit(ssize_t pos) noexcept {
    static const uint8_t andBits[] = {0x7F, 0xBF, 0xDF, 0xEF, 0xF7, 0xFB, 0xFD, 0xFE};

    size_t offset = headerLen_ + (pos >> 3);
    buf_[offset] = buf_[offset] & andBits[pos & 0x0000000000000007L];
}


bool RowWriterV2::checkNullBit(ssize_t pos) const noexcept {
    static const uint8_t bits[] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

    size_t offset = headerLen_ + (pos >> 3);
    int8_t flag = buf_[offset] & bits[pos & 0x0000000000000007L];
    return flag != 0;
}


WriteResult RowWriterV2::setValue(ssize_t index, const Value& val) noexcept {
    CHECK(!finished_) << "You have called finish()";
    if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
        return WriteResult::UNKNOWN_FIELD;
    }

    switch (val.type()) {
        case Value::Type::NULLVALUE:
            return setNull(index);
        case Value::Type::BOOL:
            return write(index, val.getBool());
        case Value::Type::INT:
            return write(index, val.getInt());
        case Value::Type::FLOAT:
            return write(index, val.getFloat());
        case Value::Type::STRING:
            return write(index, val.getStr());
        case Value::Type::DATE:
            return write(index, val.getDate());
        case Value::Type::TIME:
            return write(index, val.getTime());
        case Value::Type::DATETIME:
            return write(index, val.getDateTime());
        default:
            return WriteResult::TYPE_MISMATCH;
    }
}


WriteResult RowWriterV2::setValue(const std::string &name, const Value& val) noexcept {
    CHECK(!finished_) << "You have called finish()";
    int64_t index = schema_->getFieldIndex(name);
    return setValue(index, val);
}


WriteResult RowWriterV2::setNull(ssize_t index) noexcept {
    CHECK(!finished_) << "You have called finish()";
    if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
        return WriteResult::UNKNOWN_FIELD;
    }

    // Make sure the field is nullable
    auto field = schema_->field(index);
    if (!field->nullable()) {
        return WriteResult::NOT_NULLABLE;
    }

    setNullBit(field->nullFlagPos());
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::setNull(const std::string& name) noexcept {
    CHECK(!finished_) << "You have called finish()";
    int64_t index = schema_->getFieldIndex(name);
    return setNull(index);
}


WriteResult RowWriterV2::write(ssize_t index, bool v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::BOOL:
        case meta::cpp2::PropertyType::INT8:
            buf_[offset] = v ? 0x01 : 0;
            break;
        case meta::cpp2::PropertyType::INT64:
            buf_[offset + 7] = 0;
            buf_[offset + 6] = 0;
            buf_[offset + 5] = 0;
            buf_[offset + 4] = 0;   // fallthrough
        case meta::cpp2::PropertyType::INT32:
            buf_[offset + 3] = 0;
            buf_[offset + 2] = 0;   // fallthrough
        case meta::cpp2::PropertyType::INT16:
            buf_[offset + 1] = 0;
            buf_[offset + 0] = v ? 0x01 : 0;
            break;
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, float v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::INT8: {
            if (v > std::numeric_limits<int8_t>::max() ||
                v < std::numeric_limits<int8_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int8_t iv = v;
            buf_[offset] = iv;
            break;
        }
        case meta::cpp2::PropertyType::INT16: {
            if (v > std::numeric_limits<int16_t>::max() ||
                v < std::numeric_limits<int16_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int16_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
            break;
        }
        case meta::cpp2::PropertyType::INT32: {
            if (v > static_cast<float>(std::numeric_limits<int32_t>::max()) ||
                v < static_cast<float>(std::numeric_limits<int32_t>::min())) {
                return WriteResult::OUT_OF_RANGE;
            }
            int32_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            if (v > static_cast<float>(std::numeric_limits<int64_t>::max()) ||
                v < static_cast<float>(std::numeric_limits<int64_t>::min())) {
                return WriteResult::OUT_OF_RANGE;
            }
            int64_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(float));
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double dv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, double v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::INT8: {
            if (v > std::numeric_limits<int8_t>::max() ||
                v < std::numeric_limits<int8_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int8_t iv = v;
            buf_[offset] = iv;
            break;
        }
        case meta::cpp2::PropertyType::INT16: {
            if (v > std::numeric_limits<int16_t>::max() ||
                v < std::numeric_limits<int16_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int16_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
            break;
        }
        case meta::cpp2::PropertyType::INT32: {
            if (v > std::numeric_limits<int32_t>::max() ||
                v < std::numeric_limits<int32_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int32_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            if (v > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
                v < static_cast<double>(std::numeric_limits<int64_t>::min())) {
                return WriteResult::OUT_OF_RANGE;
            }
            int64_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            if (v > std::numeric_limits<float>::max() ||
                v < std::numeric_limits<float>::lowest()) {
                return WriteResult::OUT_OF_RANGE;
            }
            float fv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(double));
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, uint8_t v) noexcept {
    return write(index, static_cast<int8_t>(v));
}


WriteResult RowWriterV2::write(ssize_t index, int8_t v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::BOOL: {
            buf_[offset] = v == 0 ? 0x00 : 0x01;
            break;
        }
        case meta::cpp2::PropertyType::INT8: {
            buf_[offset] = v;
            break;
        }
        case meta::cpp2::PropertyType::INT16: {
            int16_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
            break;
        }
        case meta::cpp2::PropertyType::INT32: {
            int32_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            int64_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            float fv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double dv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, uint16_t v) noexcept {
    return write(index, static_cast<int16_t>(v));
}


WriteResult RowWriterV2::write(ssize_t index, int16_t v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::BOOL: {
            buf_[offset] = v == 0 ? 0x00 : 0x01;
            break;
        }
        case meta::cpp2::PropertyType::INT8: {
            if (v > std::numeric_limits<int8_t>::max() ||
                v < std::numeric_limits<int8_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int8_t iv = v;
            buf_[offset] = iv;
            break;
        }
        case meta::cpp2::PropertyType::INT16: {
            memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(int16_t));
            break;
        }
        case meta::cpp2::PropertyType::INT32: {
            int32_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            int64_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            float fv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double dv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, uint32_t v) noexcept {
    return write(index, static_cast<int32_t>(v));
}


WriteResult RowWriterV2::write(ssize_t index, int32_t v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::BOOL: {
            buf_[offset] = v == 0 ? 0x00 : 0x01;
            break;
        }
        case meta::cpp2::PropertyType::INT8: {
            if (v > std::numeric_limits<int8_t>::max() ||
                v < std::numeric_limits<int8_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int8_t iv = v;
            buf_[offset] = iv;
            break;
        }
        case meta::cpp2::PropertyType::INT16: {
            if (v > std::numeric_limits<int16_t>::max() ||
                v < std::numeric_limits<int16_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int16_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
            break;
        }
        case meta::cpp2::PropertyType::INT32: {
            memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(int32_t));
            break;
        }
        case meta::cpp2::PropertyType::TIMESTAMP: {
            // 32-bit timestamp can only support upto 2038-01-19
            auto ret = time::TimeUtils::toTimestamp(v);
            if (!ret.ok()) {
                return WriteResult::OUT_OF_RANGE;
            }
            auto ts = ret.value().getInt();
            memcpy(&buf_[offset], reinterpret_cast<void*>(&ts), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            int64_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            float fv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double dv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, uint64_t v) noexcept {
    return write(index, static_cast<int64_t>(v));
}


WriteResult RowWriterV2::write(ssize_t index, int64_t v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::BOOL: {
            buf_[offset] = v == 0 ? 0x00 : 0x01;
            break;
        }
        case meta::cpp2::PropertyType::INT8: {
            if (v > std::numeric_limits<int8_t>::max() ||
                v < std::numeric_limits<int8_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int8_t iv = v;
            buf_[offset] = iv;
            break;
        }
        case meta::cpp2::PropertyType::INT16: {
            if (v > std::numeric_limits<int16_t>::max() ||
                v < std::numeric_limits<int16_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int16_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int16_t));
            break;
        }
        case meta::cpp2::PropertyType::INT32: {
            if (v > std::numeric_limits<int32_t>::max() ||
                v < std::numeric_limits<int32_t>::min()) {
                return WriteResult::OUT_OF_RANGE;
            }
            int32_t iv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&iv), sizeof(int32_t));
            break;
        }
        case meta::cpp2::PropertyType::TIMESTAMP: {
            // 64-bit timestamp has way broader time range
            auto ret = time::TimeUtils::toTimestamp(v);
            if (!ret.ok()) {
                return WriteResult::OUT_OF_RANGE;
            }
            auto ts = ret.value().getInt();
            memcpy(&buf_[offset], reinterpret_cast<void*>(&ts), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            memcpy(&buf_[offset], reinterpret_cast<void*>(&v), sizeof(int64_t));
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            float fv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&fv), sizeof(float));
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double dv = v;
            memcpy(&buf_[offset], reinterpret_cast<void*>(&dv), sizeof(double));
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, const std::string& v) noexcept {
    return write(index, folly::StringPiece(v));
}


WriteResult RowWriterV2::write(ssize_t index, const char* v) noexcept {
    return write(index, folly::StringPiece(v));
}


WriteResult RowWriterV2::write(ssize_t index, folly::StringPiece v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::STRING: {
            if (isSet_[index]) {
                // The string value has already been set, we need to turn it
                // into out-of-space strings then
                outOfSpaceStr_ = true;
            }

            int32_t strOffset;
            int32_t strLen;
            if (outOfSpaceStr_) {
                strList_.emplace_back(v.data(), v.size());
                strOffset = 0;
                // Length field is the index to the out-of-space string list
                strLen = strList_.size() - 1;
            } else {
                // Append to the end
                strOffset = buf_.size();
                strLen = v.size();
                buf_.append(v.data(), strLen);
            }
            memcpy(&buf_[offset], reinterpret_cast<void*>(&strOffset), sizeof(int32_t));
            memcpy(&buf_[offset + sizeof(int32_t)],
                   reinterpret_cast<void*>(&strLen),
                   sizeof(int32_t));
            approxStrLen_ += v.size();
            break;
        }
        case meta::cpp2::PropertyType::FIXED_STRING: {
            // In-place string. If the pass-in string is longer than the pre-defined
            // fixed length, the string will be truncated to the fixed length
            size_t len = v.size() > field->size() ? field->size() : v.size();
            strncpy(&buf_[offset], v.data(), len);
            if (len < field->size()) {
                memset(&buf_[offset + len], 0, field->size() - len);
            }
            break;
        }
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::write(ssize_t index, const Date& v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::DATE:
            memcpy(&buf_[offset], reinterpret_cast<const void*>(&v.year), sizeof(int16_t));
            buf_[offset + sizeof(int16_t)] = v.month;
            buf_[offset + sizeof(int16_t) + sizeof(int8_t)] = v.day;
            break;
        case meta::cpp2::PropertyType::DATETIME:
            memcpy(&buf_[offset], reinterpret_cast<const void*>(&v.year), sizeof(int16_t));
            buf_[offset + sizeof(int16_t)] = v.month;
            buf_[offset + sizeof(int16_t) + sizeof(int8_t)] = v.day;
            memset(&buf_[offset + sizeof(int16_t) + 2 * sizeof(int8_t)],
                   0,
                   3 * sizeof(int8_t) + 2 * sizeof(int32_t));
            break;
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}

WriteResult RowWriterV2::write(ssize_t index, const Time& v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    switch (field->type()) {
        case meta::cpp2::PropertyType::TIME:
            buf_[offset] = v.hour;
            buf_[offset + sizeof(int8_t)] = v.minute;
            buf_[offset + 2 * sizeof(int8_t)] = v.sec;
            memcpy(&buf_[offset + 3 * sizeof(int8_t)],
                   reinterpret_cast<const void*>(&v.microsec),
                   sizeof(int32_t));
            break;
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}

WriteResult RowWriterV2::write(ssize_t index, const DateTime& v) noexcept {
    auto field = schema_->field(index);
    auto offset = headerLen_ + numNullBytes_ + field->offset();
    int16_t year = v.year;
    int8_t month = v.month;
    int8_t day = v.day;
    int8_t hour = v.hour;
    int8_t minute = v.minute;
    int8_t sec = v.sec;
    int32_t microsec = v.microsec;
    switch (field->type()) {
        case meta::cpp2::PropertyType::DATE:
            memcpy(&buf_[offset], reinterpret_cast<const void*>(&year), sizeof(int16_t));
            buf_[offset + sizeof(int16_t)] = month;
            buf_[offset + sizeof(int16_t) + sizeof(int8_t)] = day;
            break;
        case meta::cpp2::PropertyType::DATETIME:
            memcpy(&buf_[offset], reinterpret_cast<const void*>(&year), sizeof(int16_t));
            buf_[offset + sizeof(int16_t)] = month;
            buf_[offset + sizeof(int16_t) + sizeof(int8_t)] = day;
            buf_[offset + sizeof(int16_t) + 2 * sizeof(int8_t)] = hour;
            buf_[offset + sizeof(int16_t) + 3 * sizeof(int8_t)] = minute;
            buf_[offset + sizeof(int16_t) + 4 * sizeof(int8_t)] = sec;
            memcpy(&buf_[offset + sizeof(int16_t) + 5 * sizeof(int8_t)],
                   reinterpret_cast<const void*>(&microsec),
                   sizeof(int32_t));
            break;
        default:
            return WriteResult::TYPE_MISMATCH;
    }
    if (field->nullable()) {
        clearNullBit(field->nullFlagPos());
    }
    isSet_[index] = true;
    return WriteResult::SUCCEEDED;
}


WriteResult RowWriterV2::checkUnsetFields() noexcept {
    DefaultValueContext expCtx;
    for (size_t i = 0; i < schema_->getNumFields(); i++) {
        if (!isSet_[i]) {
            auto field = schema_->field(i);
            if (!field->nullable() && !field->hasDefault()) {
                // The field neither can be NULL, nor has a default value
                return WriteResult::FIELD_UNSET;
            }

            WriteResult r = WriteResult::SUCCEEDED;
            if (field->hasDefault()) {
                auto expr = field->defaultValue()->clone();
                auto defVal = Expression::eval(expr, expCtx);
                switch (defVal.type()) {
                    case Value::Type::NULLVALUE:
                        setNullBit(field->nullFlagPos());
                        break;
                    case Value::Type::BOOL:
                        r = write(i, defVal.getBool());
                        break;
                    case Value::Type::INT:
                        r = write(i, defVal.getInt());
                        break;
                    case Value::Type::FLOAT:
                        r = write(i, defVal.getFloat());
                        break;
                    case Value::Type::STRING:
                        r = write(i, defVal.getStr());
                        break;
                    case Value::Type::DATE:
                        r = write(i, defVal.getDate());
                        break;
                    case Value::Type::TIME:
                        r = write(i, defVal.getTime());
                        break;
                    case Value::Type::DATETIME:
                        r = write(i, defVal.getDateTime());
                        break;
                    default:
                        LOG(FATAL) << "Unsupported default value type: " << defVal.typeName()
                                   << ", default value: " << defVal
                                   << ", default value expr: " << field->defaultValue()->toString();
                }
            } else {
                // Set NULL
                setNullBit(field->nullFlagPos());
            }

            if (r != WriteResult::SUCCEEDED) {
                return r;
            }
        }
    }

    return WriteResult::SUCCEEDED;
}


std::string RowWriterV2::processOutOfSpace() noexcept {
    std::string temp;
    // Reserve enough space to avoid memory re-allocation
    temp.reserve(headerLen_ + numNullBytes_ + schema_->size() + approxStrLen_ + sizeof(int64_t));
    // Copy the data except the strings
    temp.append(buf_.data(), headerLen_ + numNullBytes_ + schema_->size());

    // Now let's process all strings
    for (size_t i = 0;  i < schema_->getNumFields(); i++) {
        auto field = schema_->field(i);
        if (field->type() != meta::cpp2::PropertyType::STRING) {
            continue;
        }

        size_t offset = headerLen_ + numNullBytes_ + field->offset();
        int32_t oldOffset;
        int32_t newOffset = temp.size();
        int32_t strLen;

        if (field->nullable() && checkNullBit(field->nullFlagPos())) {
            // Null string
            newOffset = strLen = 0;
        } else {
            // load the old offset and string length
            memcpy(reinterpret_cast<void*>(&oldOffset), &buf_[offset], sizeof(int32_t));
            memcpy(reinterpret_cast<void*>(&strLen),
                   &buf_[offset + sizeof(int32_t)],
                   sizeof(int32_t));

            if (oldOffset > 0) {
                temp.append(&buf_[oldOffset], strLen);
            } else {
                // Out of space string
                CHECK_LT(strLen, strList_.size());
                temp.append(strList_[strLen]);
                strLen = strList_[strLen].size();
            }
        }

        // Set the new offset and length
        memcpy(&temp[offset], reinterpret_cast<void*>(&newOffset), sizeof(int32_t));
        memcpy(&temp[offset + sizeof(int32_t)],
               reinterpret_cast<void*>(&strLen),
               sizeof(int32_t));
    }
    return temp;
}


WriteResult RowWriterV2::finish() noexcept {
    CHECK(!finished_) << "You have called finish()";

    // First to check whether all fields are set. If not, to check whether
    // it can be NULL or there is a default value for the field
    WriteResult res = checkUnsetFields();
    if (res != WriteResult::SUCCEEDED) {
        return res;
    }

    // Next to process out-of-space strings
    if (outOfSpaceStr_) {
        buf_ = processOutOfSpace();
    }

    // The timestamp will be saved to the tail of buf_
    auto ts = time::WallClock::fastNowInMicroSec();
    buf_.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));

    finished_ = true;
    return WriteResult::SUCCEEDED;
}

}  // namespace nebula
