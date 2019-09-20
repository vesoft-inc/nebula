/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman2/RowReader.h"

namespace nebula {

using nebula::meta::SchemaManager;

/*********************************************
 *
 * class RowReader::Cell
 *
 ********************************************/
ResultType RowReader::Cell::getBool(EitherOr<NullValue, bool>& v) const noexcept {
    RR_CELL_GET_VALUE(Bool);
}


ResultType RowReader::Cell::getFloat(EitherOr<NullValue, float>& v) const noexcept {
    RR_CELL_GET_VALUE(Float);
}


ResultType RowReader::Cell::getDouble(EitherOr<NullValue, double>& v) const noexcept {
    RR_CELL_GET_VALUE(Double);
}


ResultType RowReader::Cell::getString(EitherOr<NullValue, folly::StringPiece>& v)
        const noexcept {
    RR_CELL_GET_VALUE(String);
}


ResultType RowReader::Cell::getInt(EitherOr<NullValue, int64_t>& v) const noexcept {
    RR_CELL_GET_VALUE(Int);
}


ResultType RowReader::Cell::getVid(EitherOr<NullValue, int64_t>& v) const noexcept {
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


/*********************************************
 *
 * class RowReader
 *
 ********************************************/
// static
std::unique_ptr<RowReader> RowReader::getTagPropReader(
        meta::SchemaManager* schemaMan,
        folly::StringPiece row,
        GraphSpaceID space,
        TagID tag) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::make_unique<RowReader>(row, schemaMan->getTagSchema(space, tag, ver));
    } else {
        // Invalid data
        // TODO We need a better error handler here
        LOG(FATAL) << "Invalid schema version in the row data!";
        return nullptr;
    }
}


// static
std::unique_ptr<RowReader> RowReader::getEdgePropReader(
        meta::SchemaManager* schemaMan,
        folly::StringPiece row,
        GraphSpaceID space,
        EdgeType edge) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::make_unique<RowReader>(row, schemaMan->getEdgeSchema(space, edge, ver));
    } else {
        // Invalid data
        // TODO We need a better error handler here
        LOG(FATAL) << "Invalid schema version in the row data!";
        return nullptr;
    }
}


// static
int32_t RowReader::getSchemaVer(folly::StringPiece row) {
    const uint8_t* it = reinterpret_cast<const uint8_t*>(row.begin());
    if (row.size() == 0) {
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
            LOG(ERROR) << "Row data is too short";
            return 0;
        }
        // Schema Version is stored in Little Endian
        for (size_t i = 0; i < verBytes; i++) {
            ver |= (uint32_t(*(it++)) << (8 * i));
        }
    }

    return ver;
}


RowReader::RowReader(folly::StringPiece row,
                     std::shared_ptr<const meta::SchemaProviderIf> schema)
        : schema_{std::move(schema)} {
    CHECK(!!schema_) << "A schema must be provided";
    DCHECK_EQ(getSchemaVer(row), schema_->getVersion());

    if (processHeader(row)) {
        // data_.begin() points to the first field
        data_.reset(row.begin() + headerLen_, row.size() - headerLen_);
    } else {
        // Invalid data
        // TODO We need a better error handler here
        LOG(FATAL) << "Invalid row data!";
    }
}


bool RowReader::processHeader(folly::StringPiece row) {
    const uint8_t* it = reinterpret_cast<const uint8_t*>(row.begin());
    if (row.size() == 0) {
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

    // Process the NULL value flags
    const uint32_t numFields = schema_->getNumFields();
    size_t numBytesForNullFlag = numFields == 0 ? 0 : ((numFields - 1) >> 3) + 1;
    nullFlags_.resize(numBytesForNullFlag);
    memcpy(reinterpret_cast<void*>(&(nullFlags_[0])), it, numBytesForNullFlag);
    it += numBytesForNullFlag;

    // Process the block offsets
    // Block offsets point to the start of every 16 fields, except the
    // first 16 fields
    // Block offsets are stored in Little Endian
    uint32_t numOffsets = (numFields >> 4);
    if (numBytesForOffset_ * numOffsets + numBytesForNullFlag + verBytes + 1 > row.size()) {
        // Data is too short
        LOG(ERROR) << "Row data is too short"
                   << ", Total bytes for data is " << row.size()
                   << ", num bytes for null flags is " << numBytesForNullFlag
                   << ", num bytes for offset is " << numBytesForOffset_
                   << ", num offsets is " << numOffsets
                   << ", num bytes for version is " << verBytes;
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


ErrorOr<ResultType, VariantType> RowReader::getPropByName(const std::string& prop) {
    auto& vType = schema_->getFieldType(prop);
    switch (vType.type) {
        case nebula::cpp2::SupportedType::BOOL: {
            EitherOr<NullValue, bool> v;
            auto ret = getBool(prop, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::INT:
        case nebula::cpp2::SupportedType::TIMESTAMP: {
            EitherOr<NullValue, int64_t> v;
            auto ret = getInt(prop, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::VID: {
            EitherOr<NullValue, VertexID> v;
            auto ret = getVid(prop, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::FLOAT: {
            EitherOr<NullValue, float> v;
            auto ret = getFloat(prop, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return static_cast<double>(v.right());
            }
        }
        case nebula::cpp2::SupportedType::DOUBLE: {
            EitherOr<NullValue, double> v;
            auto ret = getDouble(prop, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::STRING: {
            EitherOr<NullValue, folly::StringPiece> v;
            auto ret = getString(prop, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right().toString();
            }
        }
        default: {
            LOG(FATAL) << "Unknown type: " << static_cast<int32_t>(vType.type);
            return ResultType::E_DATA_INVALID;
        }
    }
}


ErrorOr<ResultType, VariantType> RowReader::getPropByIndex(const int64_t index) {
    auto& vType = getSchema()->getFieldType(index);
    switch (vType.get_type()) {
        case nebula::cpp2::SupportedType::BOOL: {
            EitherOr<NullValue, bool> v;
            auto ret = getBool(index, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::INT:
        case nebula::cpp2::SupportedType::TIMESTAMP: {
            EitherOr<NullValue, int64_t> v;
            auto ret = getInt(index, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::VID: {
            EitherOr<NullValue, VertexID> v;
            auto ret = getVid(index, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::FLOAT: {
            EitherOr<NullValue, float> v;
            auto ret = getFloat(index, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return static_cast<double>(v.right());
            }
        }
        case nebula::cpp2::SupportedType::DOUBLE: {
            EitherOr<NullValue, double> v;
            auto ret = getDouble(index, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right();
            }
        }
        case nebula::cpp2::SupportedType::STRING: {
            EitherOr<NullValue, folly::StringPiece> v;
            auto ret = getString(index, v);
            if (ret != ResultType::SUCCEEDED) {
                return ret;
            }
            if (v.isLeftType()) {
                return v.left();
            } else {
                return v.right().toString();
            }
        }
        default: {
            LOG(FATAL) << "Unknown type: " << static_cast<int32_t>(vType.get_type());
            return ResultType::E_DATA_INVALID;
        }
    }
}


bool RowReader::isNull(const std::string& prop) const noexcept {
    int64_t index = getSchema()->getFieldIndex(prop);
    return isNull(index);
}


bool RowReader::isNull(const int64_t index) const noexcept {
    if (index < 0) {
        LOG(ERROR) << "Field index " << index << " is out of the range";
        return true;
    }
    size_t flagIdx = index >> 3;
    DCHECK_GT(nullFlags_.size(), flagIdx);
    return nullFlags_[flagIdx] & (static_cast<uint8_t>(0x80) >> (index & 0x0000000000000007));
}


int64_t RowReader::skipToNext(int64_t index, int64_t offset) const noexcept {
    const cpp2::ValueType& vType = schema_->getFieldType(index);
    CHECK(vType != CommonConstants::kInvalidValueType())
        << "No schema for the index " << index;
    if (offsets_[index + 1] >= 0) {
        return offsets_[index + 1];
    }

    // Check whether it's a NULL value
    if (isNull(index)) {
        offset++;
    } else {
        // Not a NULL, check type
        switch (vType.get_type()) {
            case cpp2::SupportedType::BOOL: {
                // One byte
                offset++;
                break;
            }
            case cpp2::SupportedType::INT:
            case cpp2::SupportedType::TIMESTAMP: {
                int64_t v;
                int32_t len = readInteger(offset, v);
                if (len <= 0) {
                    return static_cast<int64_t>(ResultType::E_DATA_INVALID);
                }
                offset += len;
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                // Eight bytes
                offset += sizeof(float);
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                // Eight bytes
                offset += sizeof(double);
                break;
            }
            case cpp2::SupportedType::STRING: {
                int64_t strLen;
                int32_t intLen = readInteger(offset, strLen);
                if (intLen <= 0) {
                    return static_cast<int64_t>(ResultType::E_DATA_INVALID);
                }
                offset += intLen + strLen;
                break;
            }
            case cpp2::SupportedType::VID: {
                // Eight bytes
                offset += sizeof(int64_t);
                break;
            }
            default: {
                // TODO
                LOG(FATAL) << "Unimplemented";
            }
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
    DCHECK_GE(index, 0);
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


int32_t RowReader::readInteger(int64_t offset, int64_t& v) const noexcept {
    const uint8_t* start = reinterpret_cast<const uint8_t*>(&(data_[offset]));
    folly::ByteRange range(start, data_.size() - offset);

    // TODO We might want to catch the exception here
    v = folly::decodeVarint(range);
    return range.begin() - start;
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


int32_t RowReader::readString(int64_t offset, folly::StringPiece& v)
        const noexcept {
    int64_t strLen;
    int32_t intLen = readInteger(offset, strLen);
    CHECK_GT(intLen, 0) << "Invalid string length";
    if (offset + intLen + strLen > static_cast<int64_t>(data_.size())) {
        return static_cast<int32_t>(ResultType::E_DATA_INVALID);
    }

    v = data_.subpiece(offset + intLen, strLen);
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


ResultType RowReader::getBool(int64_t index,
                              int64_t& offset,
                              EitherOr<NullValue, bool>& v) const noexcept {
    if (isNull(index)) {
        v = NullValue(static_cast<NullValue::NullType>(data_[offset++]));
    } else {
        switch (schema_->getFieldType(index).get_type()) {
            case cpp2::SupportedType::BOOL: {
                v = intToBool(data_[offset]);
                offset++;
                break;
            }
            case cpp2::SupportedType::INT:
            case cpp2::SupportedType::TIMESTAMP: {
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
            case cpp2::SupportedType::STRING: {
                folly::StringPiece strV;
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
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getFloat(int64_t index,
                               int64_t& offset,
                               EitherOr<NullValue, float>& v) const noexcept {
    if (isNull(index)) {
        v = NullValue(static_cast<NullValue::NullType>(data_[offset++]));
    } else {
        switch (schema_->getFieldType(index).get_type()) {
            case cpp2::SupportedType::FLOAT: {
                float fVal;
                int32_t numBytes = readFloat(offset, fVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = fVal;
                offset += numBytes;
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                double dVal;
                int32_t numBytes = readDouble(offset, dVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = static_cast<float>(dVal);
                offset += numBytes;
                break;
            }
            default: {
                return ResultType::E_INCOMPATIBLE_TYPE;
            }
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getDouble(int64_t index,
                                int64_t& offset,
                                EitherOr<NullValue, double>& v) const noexcept {
    if (isNull(index)) {
        v = NullValue(static_cast<NullValue::NullType>(data_[offset++]));
    } else {
        switch (schema_->getFieldType(index).get_type()) {
            case cpp2::SupportedType::FLOAT: {
                float fVal;
                int32_t numBytes = readFloat(offset, fVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = static_cast<double>(fVal);
                offset += numBytes;
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                double dVal;
                int32_t numBytes = readDouble(offset, dVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = dVal;
                offset += numBytes;
                break;
            }
            default: {
                return ResultType::E_INCOMPATIBLE_TYPE;
            }
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getString(int64_t index,
                                int64_t& offset,
                                EitherOr<NullValue, folly::StringPiece>& v) const noexcept {
    if (isNull(index)) {
        v = NullValue(static_cast<NullValue::NullType>(data_[offset++]));
    } else {
        switch (schema_->getFieldType(index).get_type()) {
            case cpp2::SupportedType::STRING: {
                folly::StringPiece sp;
                int32_t numBytes = readString(offset, sp);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = sp;
                offset += numBytes;
                break;
            }
            default: {
                return ResultType::E_INCOMPATIBLE_TYPE;
            }
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getInt64(int64_t index,
                               int64_t& offset,
                               EitherOr<NullValue, int64_t>& v) const noexcept {
    if (isNull(index)) {
        v = NullValue(static_cast<NullValue::NullType>(data_[offset++]));
    } else {
        int64_t iVal;
        switch (schema_->getFieldType(index).get_type()) {
            case cpp2::SupportedType::INT:
            case cpp2::SupportedType::TIMESTAMP: {
                int32_t numBytes = readInteger(offset, iVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = iVal;
                offset += numBytes;
                break;
            }
            case cpp2::SupportedType::VID: {
                int32_t numBytes = readVid(offset, iVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = iVal;
                offset += numBytes;
                break;
            }
            default: {
                return ResultType::E_INCOMPATIBLE_TYPE;
            }
        }
    }

    return ResultType::SUCCEEDED;
}


ResultType RowReader::getVid(int64_t index,
                             int64_t& offset,
                             EitherOr<NullValue, int64_t>& v) const noexcept {
    auto fieldType = schema_->getFieldType(index).get_type();
    if (fieldType == cpp2::SupportedType::INT || fieldType == cpp2::SupportedType::VID)
        return getInt64(index, offset, v);
    else
        return ResultType::E_INCOMPATIBLE_TYPE;
}


ResultType RowReader::getInt(int64_t index,
                             int64_t& offset,
                             EitherOr<NullValue, int64_t>& v) const noexcept {
    if (isNull(index)) {
        v = NullValue(static_cast<NullValue::NullType>(data_[offset++]));
    } else {
        switch (schema_->getFieldType(index).get_type()) {
            case cpp2::SupportedType::INT:
            case cpp2::SupportedType::TIMESTAMP: {
                int64_t iVal;
                int32_t numBytes = readInteger(offset, iVal);
                if (numBytes < 0) {
                    return static_cast<ResultType>(numBytes);
                }
                v = iVal;
                offset += numBytes;
                break;
            }
            default: {
                return ResultType::E_INCOMPATIBLE_TYPE;
            }
        }
    }

    return ResultType::SUCCEEDED;
}


RowReader::Iterator RowReader::begin() const noexcept {
    return Iterator(this, schema_->getNumFields(), 0);
}


RowReader::Iterator RowReader::end() const noexcept {
    auto numFields = schema_->getNumFields();
    return Iterator(this, numFields, numFields);
}


int64_t RowReader::getOffset(int64_t index) const noexcept {
    if (index < 0 || index >= static_cast<int64_t>(schema_->getNumFields())) {
        return static_cast<int64_t>(ResultType::E_INDEX_OUT_OF_RANGE);
    }

    return skipToField(index);
}


/***************************************************
 *
 * Field Accessors
 *
 **************************************************/
ResultType RowReader::getBool(const folly::StringPiece name,
                              EitherOr<NullValue, bool>& v) const noexcept {
    int64_t index = schema_->getFieldIndex(name);
    if (index < 0) {
        return ResultType::E_NAME_NOT_FOUND;
    }

    return getBool(index, v);
}


ResultType RowReader::getBool(int64_t index, EitherOr<NullValue, bool>& v)
        const noexcept {
    int64_t offset = getOffset(index);
    if (offset < 0) {
        return static_cast<ResultType>(offset);
    }

    return getBool(index, offset, v);
}


ResultType RowReader::getFloat(const folly::StringPiece name,
                               EitherOr<NullValue, float>& v) const noexcept {
    int64_t index = schema_->getFieldIndex(name);
    if (index < 0) {
        return ResultType::E_NAME_NOT_FOUND;
    }

    return getFloat(index, v);
}


ResultType RowReader::getFloat(int64_t index, EitherOr<NullValue, float>& v)
        const noexcept {
    int64_t offset = getOffset(index);
    if (offset < 0) {
        return static_cast<ResultType>(offset);
    }

    return getFloat(index, offset, v);
}


ResultType RowReader::getDouble(const folly::StringPiece name,
                                EitherOr<NullValue, double>& v) const noexcept {
    int64_t index = schema_->getFieldIndex(name);
    if (index < 0) {
        return ResultType::E_NAME_NOT_FOUND;
    }

    return getDouble(index, v);
}


ResultType RowReader::getDouble(int64_t index, EitherOr<NullValue, double>& v)
        const noexcept {
    int64_t offset = getOffset(index);
    if (offset < 0) {
        return static_cast<ResultType>(offset);
    }

    return getDouble(index, offset, v);
}


ResultType RowReader::getString(const folly::StringPiece name,
                                EitherOr<NullValue, folly::StringPiece>& v) const noexcept {
    int64_t index = schema_->getFieldIndex(name);
    if (index < 0) {
        return ResultType::E_NAME_NOT_FOUND;
    }

    return getString(index, v);
}


ResultType RowReader::getString(int64_t index, EitherOr<NullValue, folly::StringPiece>& v)
        const noexcept {
    int64_t offset = getOffset(index);
    if (offset < 0) {
        return static_cast<ResultType>(offset);
    }

    return getString(index, offset, v);
}


ResultType RowReader::getVid(const folly::StringPiece name,
                             EitherOr<NullValue, int64_t>& v) const noexcept {
    int64_t index = schema_->getFieldIndex(name);
    if (index < 0) {
        return ResultType::E_NAME_NOT_FOUND;
    }

    return getVid(index, v);
}


ResultType RowReader::getVid(int64_t index, EitherOr<NullValue, int64_t>& v)
        const noexcept {
    int64_t offset = getOffset(index);
    if (offset < 0) {
        return static_cast<ResultType>(offset);
    }

    return getVid(index, offset, v);
}


ResultType RowReader::getInt(const folly::StringPiece name,
                             EitherOr<NullValue, int64_t>& v) const noexcept {
    int64_t index = schema_->getFieldIndex(name);
    if (index < 0) {
        return ResultType::E_NAME_NOT_FOUND;
    }

    return getInt(index, v);
}


ResultType RowReader::getInt(int64_t index, EitherOr<NullValue, int64_t>& v)
        const noexcept {
    int64_t offset = getOffset(index);
    if (offset < 0) {
        return static_cast<ResultType>(offset);
    }

    return getInt(index, offset, v);
}

}  // namespace nebula
