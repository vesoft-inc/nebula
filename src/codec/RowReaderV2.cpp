/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/RowReaderV2.h"

namespace nebula {

bool RowReaderV2::resetImpl(meta::SchemaProviderIf const* schema,
                            folly::StringPiece row) noexcept {
    RowReader::resetImpl(schema, row);

    DCHECK(!!schema_);

    size_t numVerBytes = data_[0] & 0x07;
    headerLen_ = numVerBytes + 1;

#ifndef NDEBUG
    // Get the schema version
    SchemaVer schemaVer = 0;
    if (numVerBytes > 0) {
        memcpy(reinterpret_cast<void*>(&schemaVer), &data_[1], numVerBytes);
    }
    DCHECK_EQ(schemaVer, schema_->getVersion());
#endif

    // Null flags
    size_t numNullables = schema_->getNumNullableFields();
    if (numNullables > 0) {
        numNullBytes_ = ((numNullables - 1) >> 3) + 1;
    } else {
        numNullBytes_ = 0;
    }

    return true;
}


bool RowReaderV2::isNull(size_t pos) const {
    static const uint8_t bits[] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

    size_t offset = headerLen_ + (pos >> 3);
    int8_t flag = data_[offset] & bits[pos & 0x0000000000000007L];
    return flag != 0;
}


Value RowReaderV2::getValueByName(const std::string& prop) const noexcept {
    int64_t index = schema_->getFieldIndex(prop);
    return getValueByIndex(index);
}


Value RowReaderV2::getValueByIndex(const int64_t index) const noexcept {
    if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
        return Value(NullType::UNKNOWN_PROP);
    }

    auto field = schema_->field(index);
    size_t offset = headerLen_ + numNullBytes_ + field->offset();

    if (field->nullable() && isNull(field->nullFlagPos())) {
        return NullType::__NULL__;
    }

    switch (field->type()) {
        case meta::cpp2::PropertyType::BOOL: {
            if (data_[offset]) {
                return true;
            } else {
                return false;
            }
        }
        case meta::cpp2::PropertyType::INT8: {
            return static_cast<int8_t>(data_[offset]);
        }
        case meta::cpp2::PropertyType::INT16: {
            int16_t val;
            memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(int16_t));
            return val;
        }
        case meta::cpp2::PropertyType::INT32: {
            int32_t val;
            memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(int32_t));
            return val;
        }
        case meta::cpp2::PropertyType::INT64: {
            int64_t val;
            memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(int64_t));
            return val;
        }
        case meta::cpp2::PropertyType::VID: {
            // This is to be compatible with V1, so we treat it as
            // 8-byte long string
            return std::string(&data_[offset], sizeof(int64_t));
        }
        case meta::cpp2::PropertyType::FLOAT: {
            float val;
            memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(float));
            return val;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double val;
            memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(double));
            return val;
        }
        case meta::cpp2::PropertyType::STRING: {
            int32_t strOffset;
            int32_t strLen;
            memcpy(reinterpret_cast<void*>(&strOffset), &data_[offset], sizeof(int32_t));
            memcpy(reinterpret_cast<void*>(&strLen),
                   &data_[offset + sizeof(int32_t)],
                   sizeof(int32_t));
            if (static_cast<size_t>(strOffset) == data_.size() && strLen == 0) {
                return std::string();
            }
            CHECK_LT(strOffset, data_.size());
            return std::string(&data_[strOffset], strLen);
        }
        case meta::cpp2::PropertyType::FIXED_STRING: {
            return std::string(&data_[offset], field->size());
        }
        case meta::cpp2::PropertyType::TIMESTAMP: {
            Timestamp ts;
            memcpy(reinterpret_cast<void*>(&ts), &data_[offset], sizeof(Timestamp));
            return ts;
        }
        case meta::cpp2::PropertyType::DATE: {
            Date dt;
            memcpy(reinterpret_cast<void*>(&dt.year), &data_[offset], sizeof(int16_t));
            memcpy(reinterpret_cast<void*>(&dt.month),
                   &data_[offset + sizeof(int16_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&dt.day),
                   &data_[offset + sizeof(int16_t) + sizeof(int8_t)],
                   sizeof(int8_t));
            return dt;
        }
        case meta::cpp2::PropertyType::TIME: {
            Time t;
            memcpy(reinterpret_cast<void*>(&t.hour),
                   &data_[offset],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&t.minute),
                   &data_[offset + sizeof(int8_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&t.sec),
                   &data_[offset + 2 * sizeof(int8_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&t.microsec),
                   &data_[offset + 3 * sizeof(int8_t)],
                   sizeof(int32_t));
            return t;
        }
        case meta::cpp2::PropertyType::DATETIME: {
            DateTime dt;
            int16_t year;
            int8_t month;
            int8_t day;
            int8_t hour;
            int8_t minute;
            int8_t sec;
            int32_t microsec;
            memcpy(reinterpret_cast<void*>(&year), &data_[offset], sizeof(int16_t));
            memcpy(reinterpret_cast<void*>(&month),
                   &data_[offset + sizeof(int16_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&day),
                   &data_[offset + sizeof(int16_t) + sizeof(int8_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&hour),
                   &data_[offset + sizeof(int16_t) + 2 * sizeof(int8_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&minute),
                   &data_[offset + sizeof(int16_t) + 3 * sizeof(int8_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&sec),
                   &data_[offset + sizeof(int16_t) + 4 * sizeof(int8_t)],
                   sizeof(int8_t));
            memcpy(reinterpret_cast<void*>(&microsec),
                   &data_[offset + sizeof(int16_t) + 5 * sizeof(int8_t)],
                   sizeof(int32_t));
            dt.year = year;
            dt.month = month;
            dt.day = day;
            dt.hour = hour;
            dt.minute = minute;
            dt.sec = sec;
            dt.microsec = microsec;
            return dt;
        }
        case meta::cpp2::PropertyType::UNKNOWN:
            break;
    }
    LOG(FATAL) << "Should not reach here";
}

int64_t RowReaderV2::getTimestamp() const noexcept {
    return *reinterpret_cast<const int64_t*>(data_.begin() + (data_.size() - sizeof(int64_t)));
}

}  // namespace nebula
