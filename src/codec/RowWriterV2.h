/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWWRITERV2_H_
#define CODEC_ROWWRITERV2_H_

#include "common/base/Base.h"
#include "common/meta/SchemaProviderIf.h"
#include "codec/RowReader.h"

namespace nebula {

enum class WriteResult {
    SUCCEEDED = 0,
    UNKNOWN_FIELD = -1,
    TYPE_MISMATCH = -2,
    OUT_OF_RANGE = -3,
    NOT_NULLABLE = -4,
    FIELD_UNSET = -5,
    INCORRECT_VALUE = -6,
};


/********************************************************************************

  Encoder version 2

  This is the second version of encoder. It is not compatible with the first
  version. The purpose of this new version is to improve the read performance

  The first byte of the encoded string indicates which version of the encoder
  used to encode the properties. Here is the explanation of the header byte

  Version 1:
                 v v v 0 0 b b b
    In version 1, the middle two bits are always zeros. The left three bits
    indicats the number of bytes used for the schema version, while the right
    three bits indicates the number of bytes used for the block offsets

  Version 2:
                 0 0 0 0 1 v v v
    In version 2, the left three bits are reserved. The middle two bits
    indicate the encoder version, and the right three bits indicate the number
    of bytes used for the schema version

  The biggest change from version 1 to version 2 is every property now has a
  fixed length. As long as you know the property type, you know how many bytes
  the property occupies. In other words, given the sequence number of the
  property, you get the start point of the property in O(1) time.

  The types supported by the encoder version 2 are:
        BOOL            (1 byte)
        INT8            (1 byte)
        INT16           (2 bytes)
        INT32           (4 bytes)
        INT64           (8 bytes)
        FLOAT           (4 bytes)
        DOUBLE          (8 bytes)
        STRING          (8 bytes) *
        FIXED_STRING    (Length defined in the schema)
        TIMESTAMP       (8 bytes)
        DATE            (4 bytes)
        DATETIME        (15 bytes)

  All except STRING typed properties are stored in-place. The STRING property
  stored the offset of the string content in the first 4 bytes and the length
  of the string in the last 4 bytes. The string content is appended to the end
  of the encoded string

  The encoder version 2 also supports the NULL value for all types. It uses
  one bit flag to indicate whether a property is NULL if the property is
  nullable. So one byte can represent NULL values for 8 nullable properties.
  The total number of bytes needed for the NULL flags is

  ((number_of_nullable_props - 1) >> 3) + 1

  Here is the overall byte sequence for the version 2 encoding

    <header> <schema version> <NULL flags> <all properties> <string content>
       |             |             |              |
     1 byte     0 - 7 bytes     0+ bytes       N bytes

********************************************************************************/
class RowWriterV2 {
public:
    explicit RowWriterV2(const meta::SchemaProviderIf* schema);
    // This constructor only takes a V2 encoded string
    RowWriterV2(const meta::SchemaProviderIf* schema, std::string&& encoded);
    // This constructor only takes a V2 encoded string
    RowWriterV2(const meta::SchemaProviderIf* schema, const std::string& encoded);
    // This constructor can handle both V1 and V2 readers
    explicit RowWriterV2(RowReader& reader);

    ~RowWriterV2() = default;

    // Return the exact length of the encoded binary array
    int64_t size() const noexcept {
        return buf_.size();
    }

    const meta::SchemaProviderIf* schema() const {
        return schema_;
    }

    const std::string& getEncodedStr() const noexcept {
        CHECK(finished_) << "You need to call finish() first";
        return buf_;
    }

    std::string moveEncodedStr() noexcept {
        CHECK(finished_) << "You need to call finish() first";
        return std::move(buf_);
    }

    WriteResult finish() noexcept;

    // Data write
    template<typename T>
    WriteResult set(size_t index, T&& v) noexcept {
        CHECK(!finished_) << "You have called finish()";
        if (index >= schema_->getNumFields()) {
            return WriteResult::UNKNOWN_FIELD;
        }
        return write(index, std::forward<T>(v));
    }

    // Data write
    template<typename T>
    WriteResult set(const std::string &name, T&& v) noexcept {
        CHECK(!finished_) << "You have called finish()";
        int64_t index = schema_->getFieldIndex(name);
        if (index >= 0) {
            return write(static_cast<size_t>(index), std::forward<T>(v));
        } else {
            return WriteResult::UNKNOWN_FIELD;
        }
    }

    WriteResult setValue(ssize_t index, const Value& val) noexcept;
    WriteResult setValue(const std::string &name, const Value& val) noexcept;

    WriteResult setNull(ssize_t index) noexcept;
    WriteResult setNull(const std::string &name) noexcept;

private:
    const meta::SchemaProviderIf* schema_;
    std::string buf_;
    std::vector<bool> isSet_;
    // Ther number of bytes ocupied by header and the schema version
    size_t headerLen_;
    size_t numNullBytes_;
    size_t approxStrLen_;
    bool finished_;

    // When outOfSpaceStr_ is true, variant length string fields
    // could hold an index, referring to the strings in the strList_
    // By default, outOfSpaceStr_ is false. It turns true only when
    // the existing variant length string is modified
    bool outOfSpaceStr_;
    std::vector<std::string> strList_;

    WriteResult checkUnsetFields() noexcept;
    std::string processOutOfSpace() noexcept;

    void processV2EncodedStr() noexcept;

    void setNullBit(ssize_t pos) noexcept;
    void clearNullBit(ssize_t pos) noexcept;
    // Return true if the flag at the given position is NULL;
    // otherwise, return false
    bool checkNullBit(ssize_t pos) const noexcept;

    WriteResult write(ssize_t index, bool v) noexcept;
    WriteResult write(ssize_t index, float v) noexcept;
    WriteResult write(ssize_t index, double v) noexcept;

    WriteResult write(ssize_t index, int8_t v) noexcept;
    WriteResult write(ssize_t index, int16_t v) noexcept;
    WriteResult write(ssize_t index, int32_t v) noexcept;
    WriteResult write(ssize_t index, int64_t v) noexcept;
    WriteResult write(ssize_t index, uint8_t v) noexcept;
    WriteResult write(ssize_t index, uint16_t v) noexcept;
    WriteResult write(ssize_t index, uint32_t v) noexcept;
    WriteResult write(ssize_t index, uint64_t v) noexcept;

    WriteResult write(ssize_t index, const std::string& v) noexcept;
    WriteResult write(ssize_t index, folly::StringPiece v) noexcept;
    WriteResult write(ssize_t index, const char* v) noexcept;

    WriteResult write(ssize_t index, const Date& v) noexcept;
    WriteResult write(ssize_t index, const Time& v) noexcept;
    WriteResult write(ssize_t index, const DateTime& v) noexcept;
};

}  // namespace nebula
#endif  // CODEC_ROWWRITERV2_H_

