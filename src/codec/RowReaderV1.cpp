/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/RowReaderV1.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include <thrift/lib/cpp/util/EnumUtils.h>


#define RR_GET_OFFSET()                                             \
    if (index >= static_cast<int64_t>(schema_->getNumFields())) {   \
        return NullType::BAD_DATA;                                  \
    }                                                               \
    int64_t offset = skipToField(index);                            \
    if (offset < 0) {                                               \
        return NullType::BAD_DATA;                                  \
    }

namespace nebula {

/*********************************************
 *
 * class RowReaderV1
 *
 ********************************************/
bool RowReaderV1::resetImpl(meta::SchemaProviderIf const* schema,
                            folly::StringPiece row) noexcept {
    RowReader::resetImpl(schema, row);

    DCHECK(schema_ != nullptr) << "A schema must be provided";

    headerLen_ = 0;
    numBytesForOffset_ = 0;
    blockOffsets_.clear();
    offsets_.clear();

    if (processHeader(data_)) {
        // buffer_.begin() points to the first field
        buffer_ = data_.subpiece(headerLen_);
        return true;
    } else {
        // Invalid data
        LOG(ERROR) << "Invalid row data: " << toHexStr(row);
        return false;
    }
}

bool RowReaderV1::processHeader(folly::StringPiece row) {
    const uint8_t* it = reinterpret_cast<const uint8_t*>(row.begin());
    if (reinterpret_cast<const char*>(it) == row.end()) {
        return false;
    }

    DCHECK(schema_ != nullptr) << "A schema must be provided";

    // The last three bits indicate the number of bytes for offsets
    // The first three bits indicate the number of bytes for the
    // schema version. If the number is zero, no schema version
    // presents
    numBytesForOffset_ = (*it & 0x07) + 1;
    int32_t verBytes = *(it++) >> 5;
    it += verBytes;

    // Process the block offsets
    // Block offsets point to the start of every 16 fields, except the
    // first 16 fields
    // Block offsets are stored in Little Endian
    uint32_t numFields = schema_->getNumFields();
    uint32_t numOffsets = (numFields >> 4);
    if (numBytesForOffset_ * numOffsets + verBytes + 1 > row.size()) {
        // Data is too short
        LOG(ERROR) << "Row data is too short: " << toHexStr(row);
        return false;
    }
    offsets_.resize(numFields + 1, -1);
    offsets_[0] = 0;
    blockOffsets_.emplace_back(0, 0);
    blockOffsets_.reserve(numOffsets);
    for (uint32_t i = 0; i < numOffsets; i++) {
        int64_t offset = 0;
        for (int32_t j = 0; j < numBytesForOffset_; j++) {
            offset |= (uint64_t(*(it++)) << (8 * j));
        }
        blockOffsets_.emplace_back(offset, 0);
        offsets_[16 * (i + 1)] = offset;
    }
    // Now done with the header

    headerLen_ = reinterpret_cast<const char*>(it) - row.begin();
    offsets_[numFields] = row.size() - headerLen_;

    return true;
}


int64_t RowReaderV1::skipToNext(int64_t index, int64_t offset) const noexcept {
    const meta::cpp2::PropertyType& vType = getSchema()->getFieldType(index);
    if (offsets_[index + 1] >= 0) {
        return offsets_[index + 1];
    }

    switch (vType) {
        case meta::cpp2::PropertyType::BOOL: {
            // One byte
            offset++;
            break;
        }
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP: {
            int64_t v;
            int32_t len = readInteger(offset, v);
            if (len <= 0) {
                return -1;
            }
            offset += len;
            break;
        }
        case meta::cpp2::PropertyType::FLOAT: {
            // Four bytes
            offset += sizeof(float);
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            // Eight bytes
            offset += sizeof(double);
            break;
        }
        case meta::cpp2::PropertyType::STRING: {
            int64_t strLen;
            int32_t intLen = readInteger(offset, strLen);
            if (intLen <= 0) {
                return -1;
            }
            offset += intLen + strLen;
            break;
        }
        case meta::cpp2::PropertyType::VID: {
            // Eight bytes
            offset += sizeof(int64_t);
            break;
        }
        default: {
            // TODO
            LOG(FATAL) << "Unimplemented";
        }
    }

    if (offset > static_cast<int64_t>(buffer_.size())) {
        return -1;
    }

    // Update offsets
    offsets_[index + 1] = offset;
    // Update block offsets
    int32_t base = (index + 1) >> 4;
    blockOffsets_[base].second = ((index + 1) & 0x0F);

    return offset;
}


int64_t RowReaderV1::skipToField(int64_t index) const noexcept {
    DCHECK_GE(index, 0);
    if (index >= static_cast<int64_t>(schema_->getNumFields())) {
        // Index is out of range
        return -1;
    }

    int64_t base = index >> 4;
    const auto& blockOffset = blockOffsets_[base];
    base <<= 4;
    int64_t maxVisitedIndex = base + blockOffset.second;
    if (index <= maxVisitedIndex) {
        return offsets_[index];
    }

    int64_t offset = offsets_[maxVisitedIndex];
    for (int64_t i = maxVisitedIndex; i < base + (index & 0x0000000f); i++) {
        offset = skipToNext(i, offset);
        if (offset < 0) {
            return -1;
        }
    }

    return offset;
}


/************************************************************
 *
 *  Get the property value
 *
 ***********************************************************/
Value RowReaderV1::getValueByName(const std::string& prop) const noexcept {
    int64_t index = getSchema()->getFieldIndex(prop);
    return getValueByIndex(index);
}


Value RowReaderV1::getValueByIndex(const int64_t index) const noexcept {
    if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
        return Value(NullType::UNKNOWN_PROP);
    }
    auto vType = getSchema()->getFieldType(index);
    if (meta::cpp2::PropertyType::UNKNOWN == vType) {
        return Value(NullType::UNKNOWN_PROP);
    }
    switch (vType) {
        case meta::cpp2::PropertyType::BOOL:
            return getBool(index);
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP:
            return getInt(index);
        case meta::cpp2::PropertyType::VID:
            return getVid(index);
        case meta::cpp2::PropertyType::FLOAT:
            return getFloat(index);
        case meta::cpp2::PropertyType::DOUBLE:
            return getDouble(index);
        case meta::cpp2::PropertyType::STRING:
            return getString(index);
        default:
            LOG(ERROR) << "Unknown type: " << apache::thrift::util::enumNameSafe(vType);
            return NullType::BAD_TYPE;
    }
}

int64_t RowReaderV1::getTimestamp() const noexcept {
    return std::numeric_limits<int64_t>::max();
}

/************************************************************
 *
 *  Get the property value from the serialized binary string
 *
 ***********************************************************/
Value RowReaderV1::getBool(int64_t index) const noexcept {
    RR_GET_OFFSET()
    Value v;
    switch (getSchema()->getFieldType(index)) {
        case meta::cpp2::PropertyType::BOOL: {
            v.setBool(intToBool(buffer_[offset]));
            offset++;
            break;
        }
        case meta::cpp2::PropertyType::INT64: {
            int64_t intV;
            int32_t numBytes = readInteger(offset, intV);
            if (numBytes > 0) {
                v.setBool(intToBool(intV));
                offset += numBytes;
            } else {
                v.setNull(NullType::BAD_DATA);
            }
            break;
        }
        case meta::cpp2::PropertyType::STRING: {
            folly::StringPiece strV;
            int32_t numBytes = readString(offset, strV);
            if (numBytes > 0) {
                v.setBool(strToBool(strV));
                offset += numBytes;
            } else {
                v.setNull(NullType::BAD_DATA);
            }
            break;
        }
        default: {
            v.setNull(NullType::BAD_TYPE);
        }
    }

    return v;
}


Value RowReaderV1::getInt(int64_t index) const noexcept {
    RR_GET_OFFSET()
    Value v;
    switch (getSchema()->getFieldType(index)) {
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP: {
            int64_t val;
            int32_t numBytes = readInteger(offset, val);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setInt(val);
                offset += numBytes;
            }
            break;
        }
        default: {
            v.setNull(NullType::BAD_TYPE);
        }
    }

    return v;
}


Value RowReaderV1::getFloat(int64_t index) const noexcept {
    RR_GET_OFFSET()
    Value v;
    switch (getSchema()->getFieldType(index)) {
        case meta::cpp2::PropertyType::FLOAT: {
            float f;
            int32_t numBytes = readFloat(offset, f);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setFloat(f);
                offset += numBytes;
            }
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double d;
            int32_t numBytes = readDouble(offset, d);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                if (d < std::numeric_limits<float>::min() ||
                    d > std::numeric_limits<float>::max()) {
                    v.setNull(NullType::ERR_OVERFLOW);
                } else {
                    v.setFloat(d);
                }
                offset += numBytes;
            }
            break;
        }
        default: {
            v.setNull(NullType::BAD_TYPE);
        }
    }

    return v;
}


Value RowReaderV1::getDouble(int64_t index) const noexcept {
    RR_GET_OFFSET()
    Value v;
    switch (getSchema()->getFieldType(index)) {
        case meta::cpp2::PropertyType::FLOAT: {
            float f;
            int32_t numBytes = readFloat(offset, f);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setFloat(f);
                offset += numBytes;
            }
            break;
        }
        case meta::cpp2::PropertyType::DOUBLE: {
            double d;
            int32_t numBytes = readDouble(offset, d);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setFloat(d);
                offset += numBytes;
            }
            break;
        }
        default: {
            v.setNull(NullType::BAD_TYPE);
        }
    }

    return v;
}


Value RowReaderV1::getString(int64_t index) const noexcept {
    RR_GET_OFFSET()
    Value v;
    switch (getSchema()->getFieldType(index)) {
        case meta::cpp2::PropertyType::STRING: {
            folly::StringPiece s;
            int32_t numBytes = readString(offset, s);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setStr(s.toString());
                offset += numBytes;
            }
            break;
        }
        default: {
            v.setNull(NullType::BAD_TYPE);
        }
    }

    return v;
}


Value RowReaderV1::getInt64(int64_t index) const noexcept {
    RR_GET_OFFSET()
    Value v;
    int64_t val;
    switch (getSchema()->getFieldType(index)) {
        case meta::cpp2::PropertyType::INT64:
        case meta::cpp2::PropertyType::TIMESTAMP: {
            int32_t numBytes = readInteger(offset, val);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setInt(val);
                offset += numBytes;
            }
            break;
        }
        case meta::cpp2::PropertyType::VID: {
            int32_t numBytes = readVid(offset, val);
            if (numBytes < 0) {
                v.setNull(NullType::BAD_DATA);
            } else {
                v.setInt(val);
                offset += numBytes;
            }
            break;
        }
        default: {
            v.setNull(NullType::BAD_TYPE);
        }
    }

    return v;
}


Value RowReaderV1::getVid(int64_t index) const noexcept {
    auto fieldType = getSchema()->getFieldType(index);
    if (fieldType == meta::cpp2::PropertyType::INT64 ||
        fieldType == meta::cpp2::PropertyType::VID) {
        // Since 2.0, vid has been defined as a binary array. So we need to convert
        // the int64 vid in 1.0 to a binary array
        Value v(getInt64(index));
        CHECK_EQ(v.type(), Value::Type::INT);
        int64_t vid = v.getInt();
        v.setStr(std::string(reinterpret_cast<char*>(&vid), sizeof(int64_t)));
        return v;
    } else {
        return NullType::BAD_TYPE;
    }
}


/************************************************************
 *
 *  Low -level functions to read from the bytes
 *
 ***********************************************************/
int32_t RowReaderV1::readInteger(int64_t offset, int64_t& v) const noexcept {
    const uint8_t* start = reinterpret_cast<const uint8_t*>(&(buffer_[offset]));
    folly::ByteRange range(start, buffer_.size() - offset);

    try {
        v = folly::decodeVarint(range);
    } catch (const std::exception& ex) {
        return -1;
    }
    return range.begin() - start;
}


int32_t RowReaderV1::readFloat(int64_t offset, float& v) const noexcept {
    if (offset + sizeof(float) > buffer_.size()) {
        return -1;
    }

    memcpy(reinterpret_cast<char*>(&v), &(buffer_[offset]), sizeof(float));

    return sizeof(float);
}


int32_t RowReaderV1::readDouble(int64_t offset, double& v) const noexcept {
    if (offset + sizeof(double) > buffer_.size()) {
        return -1;
    }

    memcpy(reinterpret_cast<char*>(&v), &(buffer_[offset]), sizeof(double));

    return sizeof(double);
}


int32_t RowReaderV1::readString(int64_t offset, folly::StringPiece& v) const noexcept {
    int64_t strLen = 0;
    int32_t intLen = readInteger(offset, strLen);
    CHECK_GT(intLen, 0) << "Invalid string length";
    if (offset + intLen + strLen > static_cast<int64_t>(buffer_.size())) {
        return -1;
    }

    v = folly::StringPiece(buffer_.data() + offset + intLen, strLen);
    return intLen + strLen;
}


int32_t RowReaderV1::readInt64(int64_t offset, int64_t& v) const noexcept {
    if (offset + sizeof(int64_t) > buffer_.size()) {
        return -1;
    }

    // VID is stored in Little Endian
    memcpy(reinterpret_cast<char*>(&v), &(buffer_[offset]), sizeof(int64_t));

    return sizeof(int64_t);
}


int32_t RowReaderV1::readVid(int64_t offset, int64_t& v) const noexcept {
    return readInt64(offset, v);
}

}  // namespace nebula
