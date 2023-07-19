/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_ROWREADERV2_H_
#define CODEC_ROWREADERV2_H_

#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "common/meta/NebulaSchemaProvider.h"

namespace nebula {

class RowReaderWrapper;

/**
 * This class decodes the data from version 2.0
 */
class RowReaderV2 {
  friend class RowReaderWrapper;

  FRIEND_TEST(RowReaderV2, encodedData);

 public:
  ~RowReaderV2() = default;

  Value getValueByName(const std::string& prop) const;
  Value getValueByIndex(const int64_t index) const;
  int64_t getTimestamp() const noexcept;

  size_t headerLen() const noexcept {
    return headerLen_;
  }

  const meta::NebulaSchemaProvider* getSchema() const {
    return schema_;
  }

  SchemaVer schemaVer() const noexcept {
    return schema_->getVersion();
  }

  size_t numFields() const noexcept {
    return schema_->getNumFields();
  }

  const std::string getData() const {
    return data_.toString();
  }

 private:
  bool resetImpl(meta::NebulaSchemaProvider const* schema, folly::StringPiece row);

 private:
  meta::NebulaSchemaProvider const* schema_;
  folly::StringPiece data_;
  size_t headerLen_;
  size_t numNullBytes_;

  RowReaderV2() = default;

  // Check whether the flag at the given position is set or not
  bool isNull(size_t pos) const;
};

}  // namespace nebula
#endif  // CODEC_ROWREADERV2_H_
