/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADERV1_H_
#define CODEC_ROWREADERV1_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "codec/RowReader.h"

namespace nebula {

class RowReaderWrapper;

/**
 * This class decodes the data from version 1.0
 */
class RowReaderV1 : public RowReader {
    friend class RowReaderWrapper;

    FRIEND_TEST(RowReaderV1, headerInfo);
    FRIEND_TEST(RowReaderV1, encodedData);

public:
    ~RowReaderV1() = default;

    Value getValueByName(const std::string& prop) const noexcept override;
    Value getValueByIndex(const int64_t index) const noexcept override;
    int64_t getTimestamp() const noexcept override;

    int32_t readerVer() const noexcept override {
        return 1;
    }

    size_t headerLen() const noexcept override {
        return headerLen_;
    }

protected:
    bool resetImpl(meta::SchemaProviderIf const* schema, folly::StringPiece row)
        noexcept override;

private:
    int32_t headerLen_ = 0;
    int32_t numBytesForOffset_ = 0;
    // Block offet value is composed by two integers. The first one is
    // the block offset, the second one is the largest index being visited
    // in the block. This index is zero-based
    mutable std::vector<std::pair<int64_t, uint8_t>> blockOffsets_;
    mutable std::vector<int64_t> offsets_;
    // When we reset reader as RowReaderV1, it will trim the header, and save as buffer_.
    // The raw row is saved in data_.
    folly::StringPiece buffer_;

private:
    RowReaderV1() = default;

    // Process the row header infomation
    // Returns false when the row data is invalid
    bool processHeader(folly::StringPiece row);

    // Process the block offsets (each block contains certain number of fields)
    // Returns false when the row data is invalid
    bool processBlockOffsets(folly::StringPiece row, int32_t verBytes);

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

    // Following methods assume the parameters index are valid
    // When succeeded, offset will advance
    Value getBool(int64_t index) const noexcept;
    Value getInt(int64_t index) const noexcept;
    Value getFloat(int64_t index) const noexcept;
    Value getDouble(int64_t index) const noexcept;
    Value getString(int64_t index) const noexcept;
    Value getInt64(int64_t index) const noexcept;
    Value getVid(int64_t index) const noexcept;

    // The following methods all return the number of bytes read
    // A negative number will be returned if an error occurs
    int32_t readInteger(int64_t offset, int64_t& v) const noexcept;
    int32_t readFloat(int64_t offset, float& v) const noexcept;
    int32_t readDouble(int64_t offset, double& v) const noexcept;
    int32_t readString(int64_t offset, folly::StringPiece& v) const noexcept;
    int32_t readInt64(int64_t offset, int64_t& v) const noexcept;
    int32_t readVid(int64_t offset, int64_t& v) const noexcept;
};

}  // namespace nebula
#endif  // CODEC_ROWREADERV1_H_
