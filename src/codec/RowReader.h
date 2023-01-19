/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_ROWREADER_H_
#define CODEC_ROWREADER_H_

#include "codec/Common.h"
#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/SchemaProviderIf.h"

namespace nebula {

/**
 * @brief This class decodes one row of data
 */
class RowReader {
 public:
  class Iterator;

  class Cell final {
    friend class Iterator;

   public:
    Value value() const;

   private:
    const Iterator* iter_;

    explicit Cell(const Iterator* iter) : iter_(iter) {}
  };

  /**
   * @brief Helper class to iterate over all fields in a row
   */
  class Iterator final {
    friend class Cell;
    friend class RowReader;

   public:
    const Cell& operator*() const noexcept;
    const Cell* operator->() const noexcept;

    Iterator& operator++();

    bool operator==(const Iterator& rhs) const noexcept;
    bool operator!=(const Iterator& rhs) const noexcept {
      return !operator==(rhs);
    }

   private:
    const RowReader* reader_;
    Cell cell_;
    size_t index_;

    explicit Iterator(const RowReader* reader) : reader_(reader), cell_(this) {}

    Iterator(const RowReader* reader, size_t index) : reader_(reader), cell_(this), index_(index) {}

    void reset(size_t index = 0) {
      index_ = index;
    }
  };

 public:
  virtual ~RowReader() = default;

  /**
   * @brief Get the property value by property name
   *
   * @param prop Property name
   * @return Value Property value
   */
  virtual Value getValueByName(const std::string& prop) const = 0;

  /**
   * @brief Get the property value by index in schema
   *
   * @param index Index in Schema
   * @return Value Property value
   */
  virtual Value getValueByIndex(const int64_t index) const = 0;

  /**
   * @brief Get the timestamp in value
   *
   * @return int64_t UTC
   */
  virtual int64_t getTimestamp() const noexcept = 0;

  /**
   * @brief The row reader version, only 1 or 2 is legal
   *
   * @return int32_t Reader version
   */
  virtual int32_t readerVer() const noexcept = 0;

  /**
   * @brief Return the number of bytes used for the header info
   */
  virtual size_t headerLen() const noexcept = 0;

  /**
   * @brief Iterator points to the first field
   *
   * @return Iterator
   */
  virtual Iterator begin() const {
    return Iterator(this, 0);
  }

  /**
   * @brief Iterator points to the last filed
   *
   * @return const Iterator&
   */
  virtual const Iterator& end() const {
    return endIter_;
  }

  /**
   * @brief The schema version encoded in value
   *
   * @return SchemaVer Schema version
   */
  virtual SchemaVer schemaVer() const noexcept {
    return schema_->getVersion();
  }

  /**
   * @brief Count of fields
   *
   * @return size_t
   */
  virtual size_t numFields() const noexcept {
    return schema_->getNumFields();
  }

  /**
   * @brief Get the schema of row data
   *
   * @return const meta::SchemaProviderIf*
   */
  virtual const meta::SchemaProviderIf* getSchema() const {
    return schema_;
  }

  /**
   * @brief Get the raw value in kv engine
   *
   * @return const std::string
   */
  virtual const std::string getData() const {
    return data_.toString();
  }

 protected:
  meta::SchemaProviderIf const* schema_;
  folly::StringPiece data_;

  RowReader() : endIter_(this) {}

  /**
   * @brief Reset the row reader with schema and data
   *
   * @param schema
   * @param row
   * @return Whether reset succeed
   */
  virtual bool resetImpl(meta::SchemaProviderIf const* schema, folly::StringPiece row);

 private:
  Iterator endIter_;
};

}  // namespace nebula
#endif  // CODEC_ROWREADER_H_
