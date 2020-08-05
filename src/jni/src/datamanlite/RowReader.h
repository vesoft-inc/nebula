/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_CODEC_ROWREADER_H_
#define DATAMAN_CODEC_ROWREADER_H_

#include <vector>
#include <gtest/gtest_prod.h>
#include "base/ThriftTypes.h"
#include "datamanlite/DataCommon.h"
#include "datamanlite/Slice.h"
#include "datamanlite/SchemaProviderIf.h"

#define RR_CELL_GET_VALUE(FN) \
    if (!*iter_) { \
        /* Already reached the end, or an error happened */ \
        return ResultType::E_INDEX_OUT_OF_RANGE; \
    } \
    int64_t offset = iter_->offset_; \
    ResultType res = reader_->get ## FN(iter_->index_, offset, v); \
    if (res == ResultType::SUCCEEDED) { \
        iter_->bytes_ = offset - iter_->offset_; \
    } else { \
        /* Move iterator to the end */ \
        iter_->index_ = iter_->numFields_; \
    } \
    return res

#define RR_GET_VALUE_BY_NAME(FN, VT) \
    RowReader::get ## FN(const Slice& name, \
                         VT& v) const noexcept { \
        int64_t index = schema_->getFieldIndex(name); \
        if (index < 0) { \
            return ResultType::E_NAME_NOT_FOUND; \
        } else { \
            return get ## FN (index, v); \
        } \
    }

#define RR_GET_OFFSET() \
    int64_t offset = skipToField(index); \
    if (offset < 0) { \
        return static_cast<ResultType>(offset); \
    } \
    if (index >= static_cast<int64_t>(schema_->getNumFields())) { \
        return ResultType::E_INDEX_OUT_OF_RANGE; \
    }

namespace nebula {
namespace dataman {
namespace codec {

/**
 * This class decodes one row of data
 */
class RowReader {
    FRIEND_TEST(RowReader, headerInfo);
    FRIEND_TEST(RowReader, encodedData);
    FRIEND_TEST(RowWriter, offsetsCreation);

public:
    class Iterator;

    class Cell final {
        friend class Iterator;
    public:
        template<typename T>
        typename std::enable_if<std::is_integral<T>::value, ResultType>::type
        getInt(T& v) const noexcept;

        ResultType getBool(bool& v) const noexcept;
        ResultType getFloat(float& v) const noexcept;
        ResultType getDouble(double& v) const noexcept;
        ResultType getString(Slice& v) const noexcept;
        ResultType getVid(int64_t& v) const noexcept;
    private:
        const RowReader* reader_;
        Iterator* iter_;

        Cell(const RowReader* reader, Iterator* iter)
            : reader_(reader), iter_(iter) {}
    };
    friend class Cell;


    class Iterator final {
        friend class RowReader;
    public:
        Iterator(Iterator&& iter);
        const Cell& operator*() const;
        const Cell* operator->() const;
        Iterator& operator++();
        operator bool() const;
        bool operator==(const Iterator& rhs) const noexcept;
    private:
        const RowReader* reader_;
        const size_t numFields_;
        std::unique_ptr<Cell> cell_;
        int64_t index_ = 0;
        int32_t bytes_ = 0;
        int64_t offset_ = 0;

        Iterator(const RowReader* reader, size_t numFields, int64_t index = 0);
    };


public:
    static std::unique_ptr<RowReader> getRowReader(const Slice& row,
                                                   std::shared_ptr<const SchemaProviderIf> schema);

    virtual ~RowReader() = default;

    SchemaVer schemaVer() const noexcept;
    int32_t numFields() const noexcept;

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

    ResultType getBool(const Slice& name, bool& v) const noexcept;
    ResultType getBool(int64_t index, bool& v) const noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    getInt(const Slice& name, T& v) const noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    getInt(int64_t index, T& v) const noexcept;

    ResultType getFloat(const Slice& name, float& v) const noexcept;
    ResultType getFloat(int64_t index, float& v) const noexcept;

    ResultType getDouble(const Slice& name, double& v) const noexcept;
    ResultType getDouble(int64_t index, double& v) const noexcept;

    ResultType getString(const Slice& name, Slice& v) const noexcept;
    ResultType getString(int64_t index, Slice& v) const noexcept;

    ResultType getVid(const Slice& name, int64_t& v) const noexcept;
    ResultType getVid(int64_t index, int64_t& v) const noexcept;


    std::shared_ptr<const SchemaProviderIf> getSchema() const {
        return schema_;
    }

    // TODO getPath(const std::string& name) const noexcept;
    // TODO getPath(int64_t index) const noexcept;
    // TODO getList(const std::string& name) const noexcept;
    // TODO getList(int64_t index) const noexcept;
    // TODO getSet(const std::string& name) const noexcept;
    // TODO getSet(int64_t index) const noexcept;
    // TODO getMap(const std::string& name) const noexcept;
    // TODO getMap(int64_t index) const noexcept;

private:
    std::shared_ptr<const SchemaProviderIf> schema_;

    Slice data_;
    int32_t headerLen_ = 0;
    int32_t numBytesForOffset_ = 0;
    // Block offet value is composed by two integers. The first one is
    // the block offset, the second one is the largest index being visited
    // in the block. This index is zero-based
    mutable std::vector<std::pair<int64_t, uint8_t>> blockOffsets_;
    mutable std::vector<int64_t> offsets_;

private:
    static int32_t getSchemaVer(Slice row);

    RowReader(Slice row,
              std::shared_ptr<const SchemaProviderIf> schema);

    // Process the row header infomation
    // Returns false when the row data is invalid
    bool processHeader(Slice row);

    // Process the block offsets (each block contains certain number of fields)
    // Returns false when the row data is invalid
    bool processBlockOffsets(Slice row, int32_t verBytes);

    // Skip to the next field
    // Parameter:
    //  index   : the current field index
    //  offset  : the current offset
    // When succeeded, the method returns the offset pointing to the
    // next field
    // When failed, the method returns a negative number
    int64_t skipToNext(int64_t index, int64_t offset) const noexcept;

    // Skip to the {index}Th field
    // The method retuns the offset of the field
    // It returns a negative number when the data corrupts
    int64_t skipToField(int64_t index) const noexcept;

    // The following methods all return the number of bytes read
    // A negative number will be returned if an error occurs
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, int32_t>::type
    readInteger(int64_t offset, T& v) const noexcept;

    int32_t readFloat(int64_t offset, float& v) const noexcept;
    int32_t readDouble(int64_t offset, double& v) const noexcept;
    int32_t readString(int64_t offset, Slice& v) const noexcept;
    int32_t readInt64(int64_t offset, int64_t& v) const noexcept;
    int32_t readVid(int64_t offset, int64_t& v) const noexcept;

    // Following methods assume the parameters index and offset are valid
    // When succeeded, offset will advance
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    getInt(int64_t index, int64_t& offset, T& v) const noexcept;

    ResultType getBool(int64_t index, int64_t& offset, bool& v) const noexcept;
    ResultType getFloat(int64_t index, int64_t& offset, float& v) const noexcept;
    ResultType getDouble(int64_t index, int64_t& offset, double& v)
        const noexcept;
    ResultType getString(int64_t index, int64_t& offset, Slice& v)
        const noexcept;
    ResultType getInt64(int64_t index, int64_t& offset, int64_t& v) const noexcept;
    ResultType getVid(int64_t index, int64_t& offset, int64_t& v) const noexcept;
};

/*********************************************
 *
 * class RowReader::Cell
 *
 ********************************************/
template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RowReader::Cell::getInt(T& v) const noexcept {
    RR_CELL_GET_VALUE(Int);
}


template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RowReader::getInt(int64_t index, int64_t& offset, T& v) const noexcept {
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
        default: {
            return ResultType::E_INCOMPATIBLE_TYPE;
        }
    }

    return ResultType::SUCCEEDED;
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RR_GET_VALUE_BY_NAME(Int, T)

template<typename T>
typename std::enable_if<std::is_integral<T>::value, ResultType>::type
RowReader::getInt(int64_t index, T& v) const noexcept {
    RR_GET_OFFSET()
    return getInt(index, offset, v);
}


template<typename T>
typename std::enable_if<std::is_integral<T>::value, int32_t>::type
RowReader::readInteger(int64_t offset, T& v) const noexcept {
    const int8_t* start = reinterpret_cast<const int8_t*>(&(data_[offset]));
    uint64_t val = 0;
    auto len = decodeVarint(start, data_.size() - offset, val);
    // CHECK_GT(len, 0);
    v = val;
    return len;
}

}  // namespace codec
}  // namespace dataman
}  // namespace nebula
#endif  // DATAMAN_CODEC_ROWREADER_H_
