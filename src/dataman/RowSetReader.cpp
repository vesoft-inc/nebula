/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman/RowSetReader.h"
#include "dataman/ResultSchemaProvider.h"
#include "dataman/RowReader.h"

namespace nebula {

using nebula::meta::SchemaProviderIf;

/***********************************
 *
 * RowSetReader::Iterator class
 *
 **********************************/
RowSetReader::Iterator::Iterator(
    std::shared_ptr<const SchemaProviderIf> schema,
    const folly::StringPiece& data,
    int64_t offset)
        : schema_(schema)
        , data_(data)
        , offset_(offset) {
    len_ = prepareReader();
}


int32_t RowSetReader::Iterator::prepareReader() {
    if (offset_ < static_cast<int64_t>(data_.size())) {
        try {
            auto begin = reinterpret_cast<const uint8_t*>(data_.begin());
            folly::ByteRange range(begin + offset_, 10);
            int32_t rowLen = folly::decodeVarint(range);
            int32_t lenBytes = range.begin() - begin - offset_;
            reader_ = RowReader::getRowReader(
                data_.subpiece(offset_ + lenBytes, rowLen),
                schema_);
            return lenBytes + rowLen;
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Failed to read the row length";
            offset_ = data_.size();
            reader_.reset();
        }
    }

    return 0;
}


const RowReader& RowSetReader::Iterator::operator*() const noexcept {
    return *reader_;
}


const RowReader* RowSetReader::Iterator::operator->() const noexcept {
    return reader_.get();
}


RowSetReader::Iterator& RowSetReader::Iterator::operator++() noexcept {
    offset_ += len_;
    len_ = prepareReader();
    return *this;
}


RowSetReader::Iterator::operator bool() const noexcept {
    return offset_ < static_cast<int64_t>(data_.size());
}


bool RowSetReader::Iterator::operator==(const Iterator& rhs) {
    return schema_ == rhs.schema_ &&
           data_ == rhs.data_ &&
           offset_ == rhs.offset_;
}


/***********************************
 *
 * RowSetReader class
 *
 **********************************/

RowSetReader::RowSetReader(std::shared_ptr<const SchemaProviderIf> schema,
                           folly::StringPiece data)
        : schema_{schema}
        , data_{data} {
}


RowSetReader::Iterator RowSetReader::begin() const noexcept {
    return Iterator(schema_, data_, 0);
}


RowSetReader::Iterator RowSetReader::end() const noexcept {
    return Iterator(schema_, data_, data_.size());
}

}  // namespace nebula

