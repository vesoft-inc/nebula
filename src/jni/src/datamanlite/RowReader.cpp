/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datamanlite/RowReader.h"
#include <cstdlib>


namespace nebula {
namespace dataman {
namespace codec {


/*********************************************
 *
 * class RowReader::Cell
 *
 ********************************************/
ResultType RowReader::Cell::getBool(bool& v) const noexcept {
    RR_CELL_GET_VALUE(Bool);
}


ResultType RowReader::Cell::getFloat(float& v) const noexcept {
    RR_CELL_GET_VALUE(Float);
}


ResultType RowReader::Cell::getDouble(double& v) const noexcept {
    RR_CELL_GET_VALUE(Double);
}


ResultType RowReader::Cell::getString(Slice& v) const noexcept {
    RR_CELL_GET_VALUE(String);
}


ResultType RowReader::Cell::getVid(int64_t& v) const noexcept {
    RR_CELL_GET_VALUE(Vid);
}


/*********************************************
 *
 * class RowReader::Iterator
 *
 ********************************************/
RowReader::Iterator::Iterator(const RowReader* reader,
                              size_t numFields,
                              int64_t index)
        : reader_(reader)
        , numFields_(numFields)
        , index_(index) {
    cell_.reset(new Cell(reader_, this));
}


RowReader::Iterator::Iterator(Iterator&& iter)
    : reader_(iter.reader_)
    , numFields_(iter.numFields_)
    , cell_(std::move(iter.cell_))
    , index_(iter.index_)
    , bytes_(iter.bytes_)
    , offset_(iter.offset_) {}


const RowReader::Cell& RowReader::Iterator::operator*() const {
    return *cell_;
}


const RowReader::Cell* RowReader::Iterator::operator->() const {
    return cell_.get();
}


RowReader::Iterator& RowReader::Iterator::operator++() {
    if (*this) {
        if (bytes_ > 0) {
            offset_ += bytes_;
        } else {
            offset_ = reader_->skipToNext(index_, offset_);
            if (offset_ < 0) {
                // Something is wrong
                index_ = numFields_;
                return *this;
            }
        }

        bytes_ = 0;
        index_++;
    }

    return *this;
}


bool RowReader::Iterator::operator==(const Iterator& rhs) const noexcept {
    return reader_ == rhs.reader_ &&
           numFields_ == rhs.numFields_ &&
           index_ == rhs.index_;
}


RowReader::Iterator::operator bool() const {
    return index_ != static_cast<int64_t>(numFields_);
}


// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        const Slice& row,
        std::shared_ptr<const SchemaProviderIf> schema) {
    SchemaVer ver = getSchemaVer(row);
    CHECK_EQ(ver, schema->getVersion());
    return std::unique_ptr<RowReader>(new RowReader(row, std::move(schema)));
}


// static
int32_t RowReader::getSchemaVer(Slice row) {
    const uint8_t* it = reinterpret_cast<const uint8_t*>(row.begin());
    if (reinterpret_cast<const char*>(it) == row.end()) {
        LOG(ERROR) << "Row data is empty, so there is no schema version";
        return 0;
    }

    // The first three bits indicate the number of bytes for the
    // schema version. If the number is zero, no schema version
    // presents
    size_t verBytes = *(it++) >> 5;
    int32_t ver = 0;
    if (verBytes > 0) {
        if (verBytes + 1 > row.size()) {
            // Data is too short
            // LOG(ERROR) << "Row data is too short";
            return 0;
        }
        // Schema Version is stored in Little Endian
        for (size_t i = 0; i < verBytes; i++) {
            ver |= (uint32_t(*(it++)) << (8 * i));
        }
    }

    return ver;
}

/*********************************************
 *
 * class RowReader
 *
 ********************************************/
RowReader::RowReader(Slice row,
                     std::shared_ptr<const SchemaProviderIf> schema)
        : schema_{std::move(schema)} {
    CHECK(!!schema_) << "A schema must be provided";

    if (processHeader(row)) {
        // data_.begin() points to the first field
        data_ = Slice(row.begin() + headerLen_, row.size() - headerLen_);
    } else {
        // Invalid data
        // TODO We need a better error handler here
        LOG(FATAL) << "Invalid row data!";
    }
}


bool RowReader::processHeader(Slice row) {
    const uint8_t* it = reinterpret_cast<const uint8_t*>(row.begin());
    if (reinterpret_cast<const char*>(it) == row.end()) {
        return false;
    }

    DCHECK(!!schema_) << "A schema must be provided";

    // The last three bits indicate the number of bytes for offsets
    // The first three bits indicate the number of bytes for the
    // schena version. If the number is zero, no schema version
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
        // LOG(ERROR) << "Row data is too short";
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


int32_t RowReader::numFields() const noexcept {
    return schema_->getNumFields();
}


SchemaVer RowReader::schemaVer() const noexcept {
    return schema_->getVersion();
}


int64_t RowReader::skipToNext(int64_t index, int64_t offset) const noexcept {
    auto vType = schema_->getFieldType(index);
    if (offsets_[index + 1] >= 0) {
        return offsets_[index + 1];
    }

    switch (vType) {
        case ValueType::BOOL: {
            // One byte
            offset++;
            break;
        }
        case ValueType::INT:
        case ValueType::TIMESTAMP: {
            int64_t v;
            int32_t len = readInteger(offset, v);
            if (len <= 0) {
                return static_cast<int64_t>(ResultType::E_DATA_INVALID);
            }
            offset += len;
            break;
        }
        case ValueType::FLOAT: {
            // Four bytes
            offset += sizeof(float);
            break;
        }
        case ValueType::DOUBLE: {
            // Eight bytes
            offset += sizeof(double);
            break;
        }
        case ValueType::STRING: {
            int64_t strLen;
            int32_t intLen = readInteger(offset, strLen);
            if (intLen <= 0) {
                return static_cast<int64_t>(ResultType::E_DATA_INVALID);
            }
            offset += intLen + strLen;
            break;
        }
        case ValueType::VID: {
            // Eight bytes
            offset += sizeof(int64_t);
            break;
        }
        default: {
            // TODO
            // LOG(FATAL) << "Unimplemented";
            std::abort();
        }
    }

    if (offset > static_cast<int64_t>(data_.size())) {
        return static_cast<int64_t>(ResultType::E_DATA_INVALID);
    }

    // Update offsets
    offsets_[index + 1] = offset;
    // Update block offsets
    int32_t base = (index + 1) >> 4;
    blockOffsets_[base].second = ((index + 1) & 0x0F);

    return offset;
}


int64_t RowReader::skipToField(int64_t index) const noexcept {
    // DCHECK_GE(index, 0);
    if (index >= static_cast<int64_t>(schema_->getNumFields())) {
        // Index is out of range
        return static_cast<int64_t>(ResultType::E_INDEX_OUT_OF_RANGE);
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
            return static_cast<int64_t>(ResultType::E_DATA_INVALID);
        }
    }

    return offset;
}


int32_t RowReader::readFloat(int64_t offset, float& v) const noexcept {
    if (offset + sizeof(float) > data_.size()) {
        return static_cast<int32_t>(ResultType::E_DATA_INVALID);
    }

    memcpy(reinterpret_cast<char*>(&v), &(data_[offset]), sizeof(float));

    return sizeof(float);
}


int32_t RowReader::readDouble(int64_t offset, double& v) const noexcept {
    if (offset + sizeof(double) > data_.size()) {
        return static_cast<int32_t>(ResultType::E_DATA_INVALID);
    }

    memcpy(reinterpret_cast<char*>(&v), &(data_[offset]), sizeof(double));

    return sizeof(double);
}


int32_t RowReader::readString(int64_t offset, Slice& v)
        const noexcept {
    int64_t strLen;
    int32_t intLen = readInteger(offset, strLen);
    // CHECK_GT(intLen, 0) << "Invalid string length";
    if (offset + intLen + strLen > static_cast<int64_t>(data_.size())) {
        return static_cast<int32_t>(ResultType::E_DATA_INVALID);
    }

    v = Slice(data_.data() + offset + intLen, strLen);
    return intLen + strLen;
}


int32_t RowReader::readInt64(int64_t offset, int64_t& v) const noexcept {
    if (offset + sizeof(int64_t) > data_.size()) {
        return static_cast<int32_t>(ResultType::E_DATA_INVALID);
    }

    // VID is stored in Little Endian
    memcpy(reinterpret_cast<char*>(&v), &(data_[offset]), sizeof(int64_t));

    return sizeof(int64_t);
}


int32_t RowReader::readVid(int64_t offset, int64_t& v) const noexcept {
    return readInt64(offset, v);
}


ResultType RowReader::getBool(int64_t index, int64_t& offset, bool& v)
        const noexcept {
    switch (schema_->getFieldType(index)) {
        case ValueType::BOOL: {
            v = intToBool(data_[offset]);
            offset++;
            break;
        }
        case ValueType::INT:
        case ValueType::TIMESTAMP: {
            int64_t intV;
            int32_t numBytes = readInteger(offset, intV);
            if (numBytes > 0) {
                v = intToBool(intV);
                offset += numBytes;
            } else {
                return static_cast<ResultType>(numBytes);
            }
            break;
        }
        case ValueType::STRING: {
            Slice strV;
            int32_t numBytes = readString(offset, strV);
            if (numBytes > 0) {
                v = strToBool(strV);
                offset += numBytes;
            } else {
                return static_cast<ResultType>(numBytes);
            }
            break;
        }
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getFloat(int64_t index, int64_t& offset, float& v)
        const noexcept {
    switch (schema_->getFieldType(index)) {
        case ValueType::FLOAT: {
            int32_t numBytes = readFloat(offset, v);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            offset += numBytes;
            break;
        }
        case ValueType::DOUBLE: {
            double d;
            int32_t numBytes = readDouble(offset, d);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            v = static_cast<float>(d);
            offset += numBytes;
            break;
        }
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getDouble(int64_t index, int64_t& offset, double& v)
        const noexcept {
    switch (schema_->getFieldType(index)) {
        case ValueType::FLOAT: {
            float f;
            int32_t numBytes = readFloat(offset, f);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            v = static_cast<double>(f);
            offset += numBytes;
            break;
        }
        case ValueType::DOUBLE: {
            int32_t numBytes = readDouble(offset, v);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            offset += numBytes;
            break;
        }
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getString(int64_t index,
                                int64_t& offset,
                                Slice& v) const noexcept {
    switch (schema_->getFieldType(index)) {
        case ValueType::STRING: {
            int32_t numBytes = readString(offset, v);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            offset += numBytes;
            break;
        }
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getInt64(int64_t index, int64_t& offset, int64_t& v)
        const noexcept {
    switch (schema_->getFieldType(index)) {
        case ValueType::INT:
        case ValueType::TIMESTAMP: {
            int32_t numBytes = readInteger(offset, v);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            offset += numBytes;
            break;
        }
        case ValueType::VID: {
            int32_t numBytes = readVid(offset, v);
            if (numBytes < 0) {
                return static_cast<ResultType>(numBytes);
            }
            offset += numBytes;
            break;
        }
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getVid(int64_t index, int64_t& offset, int64_t& v)
        const noexcept {
    auto fieldType = schema_->getFieldType(index);
    if (fieldType == ValueType::INT || fieldType == ValueType::VID)
        return getInt64(index, offset, v);
    else
        return ResultType::E_INCOMPATIBLE_TYPE;
}


RowReader::Iterator RowReader::begin() const noexcept {
    return Iterator(this, schema_->getNumFields(), 0);
}


RowReader::Iterator RowReader::end() const noexcept {
    auto numFields = schema_->getNumFields();
    return Iterator(this, numFields, numFields);
}


/***************************************************
 *
 * Field Accessors
 *
 **************************************************/
ResultType RR_GET_VALUE_BY_NAME(Bool, bool)

ResultType RowReader::getBool(int64_t index, bool& v) const noexcept {
    RR_GET_OFFSET()
    return getBool(index, offset, v);
}


ResultType RR_GET_VALUE_BY_NAME(Float, float)

ResultType RowReader::getFloat(int64_t index, float& v) const noexcept {
    RR_GET_OFFSET()
    return getFloat(index, offset, v);
}


ResultType RR_GET_VALUE_BY_NAME(Double, double)

ResultType RowReader::getDouble(int64_t index, double& v) const noexcept {
    RR_GET_OFFSET()
    return getDouble(index, offset, v);
}


ResultType RR_GET_VALUE_BY_NAME(String, Slice)

ResultType RowReader::getString(int64_t index, Slice& v)
        const noexcept {
    RR_GET_OFFSET()
    return getString(index, offset, v);
}


ResultType RR_GET_VALUE_BY_NAME(Vid, int64_t)

ResultType RowReader::getVid(int64_t index, int64_t& v) const noexcept {
    RR_GET_OFFSET()
    return getVid(index, offset, v);
}

}  // namespace codec
}  // namespace dataman
}  // namespace nebula
