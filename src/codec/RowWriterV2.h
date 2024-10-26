/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_ROWWRITERV2_H_
#define CODEC_ROWWRITERV2_H_

#include "codec/RowReaderWrapper.h"
#include "common/base/Base.h"
#include "common/meta/NebulaSchemaProvider.h"

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
    indicates the number of bytes used for the schema version, while the right
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
        GEOGRAPHY       (8 bytes) *

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
  explicit RowWriterV2(const meta::NebulaSchemaProvider* schema);
  // This constructor only takes a V2 encoded string
  RowWriterV2(const meta::NebulaSchemaProvider* schema, std::string&& encoded);
  // This constructor only takes a V2 encoded string
  RowWriterV2(const meta::NebulaSchemaProvider* schema, const std::string& encoded);
  // This constructor can handle constructed from RowReaderWrapper, which is V2 reader
  explicit RowWriterV2(RowReaderWrapper& reader);

  ~RowWriterV2() = default;

  /**
   * @brief Return the exact length of the encoded binary array
   *
   * @return int64_t
   */
  int64_t size() const noexcept {
    return buf_.size();
  }

  /**
   * @brief Return the related schema
   *
   * @return const meta::NebulaSchemaProvider*
   */
  const meta::NebulaSchemaProvider* schema() const {
    return schema_;
  }

  /**
   * @brief Get the encoded string
   *
   * @return const std::string&
   */
  const std::string& getEncodedStr() const {
    CHECK(finished_) << "You need to call finish() first";
    return buf_;
  }

  /**
   * @brief Get the encoded string with move
   *
   * @return std::string
   */
  std::string moveEncodedStr() {
    CHECK(finished_) << "You need to call finish() first";
    return std::move(buf_);
  }

  /**
   * @brief Finish setting fields, begin to encode
   *
   * @return WriteResult Whether encode succeed
   */
  WriteResult finish();

  // Data write
  /**
   * @brief Set propertyfield value by index
   *
   * @tparam T
   * @param index Field index
   * @param v Value to write
   * @return WriteResult
   */
  template <typename T>
  WriteResult set(size_t index, T&& v) {
    CHECK(!finished_) << "You have called finish()";
    if (index >= schema_->getNumFields()) {
      return WriteResult::UNKNOWN_FIELD;
    }
    return write(index, std::forward<T>(v));
  }

  // Data write
  /**
   * @brief Set property value by property name
   *
   * @tparam T
   * @param name Property name
   * @param v Value to write
   * @return WriteResult
   */
  template <typename T>
  WriteResult set(const std::string& name, T&& v) {
    CHECK(!finished_) << "You have called finish()";
    int64_t index = schema_->getFieldIndex(name);
    if (index >= 0) {
      return write(static_cast<size_t>(index), std::forward<T>(v));
    } else {
      return WriteResult::UNKNOWN_FIELD;
    }
  }

  /**
   * @brief Set the value by index
   *
   * @param index
   * @param val
   * @return WriteResult
   */
  WriteResult setValue(ssize_t index, const Value& val);

  /**
   * @brief Set the value by index
   *
   * @param index
   * @param val
   * @return WriteResult
   */
  WriteResult setValue(const std::string& name, const Value& val);

  /**
   * @brief Set null by index
   *
   * @param index
   * @return WriteResult
   */
  WriteResult setNull(ssize_t index);

  /**
   * @brief Set null by property name
   *
   * @param name
   * @return WriteResult
   */
  WriteResult setNull(const std::string& name);

 private:
  const meta::NebulaSchemaProvider* schema_;
  std::string buf_;
  std::vector<bool> isSet_;
  // The number of bytes occupied by header and the schema version
  size_t headerLen_;
  size_t numNullBytes_;
  size_t approxStrLen_;
  bool finished_;
  // Limit array length
  static constexpr int32_t kMaxArraySize = 65535;

  // When outOfSpaceStr_ is true, variant length string fields
  // could hold an index, referring to the strings in the strList_
  // By default, outOfSpaceStr_ is false. It turns true only when
  // the existing variant length string is modified
  bool outOfSpaceStr_;
  std::vector<std::string> strList_;

  WriteResult checkUnsetFields();
  std::string processOutOfSpace();

  void processV2EncodedStr();

  void setNullBit(ssize_t pos);
  void clearNullBit(ssize_t pos);
  // Return true if the flag at the given position is NULL;
  // otherwise, return false
  bool checkNullBit(ssize_t pos) const;

  WriteResult write(ssize_t index, bool v);
  WriteResult write(ssize_t index, float v);
  WriteResult write(ssize_t index, double v);

  WriteResult write(ssize_t index, int8_t v);
  WriteResult write(ssize_t index, int16_t v);
  WriteResult write(ssize_t index, int32_t v);
  WriteResult write(ssize_t index, int64_t v);
  WriteResult write(ssize_t index, uint8_t v);
  WriteResult write(ssize_t index, uint16_t v);
  WriteResult write(ssize_t index, uint32_t v);
  WriteResult write(ssize_t index, uint64_t v);

  WriteResult write(ssize_t index, const std::string& v);
  WriteResult write(ssize_t index, folly::StringPiece v, bool isWKB = false);
  WriteResult write(ssize_t index, const char* v);

  WriteResult write(ssize_t index, const Date& v);
  WriteResult write(ssize_t index, const Time& v);
  WriteResult write(ssize_t index, const DateTime& v);
  WriteResult write(ssize_t index, const Duration& v);

  WriteResult write(ssize_t index, const Geography& v);
  // Supports storing ordered lists of strings, integers, and floats,
  // including LIST_STRING, LIST_INT, and LIST_FLOAT.
  WriteResult write(ssize_t index, const List& list);
  // Supports storing unordered sets of strings, integers, and floats,
  // including SET_STRING, SET_INT, and SET_FLOAT
  WriteResult write(ssize_t index, const Set& set);
};

}  // namespace nebula
#endif  // CODEC_ROWWRITERV2_H_
