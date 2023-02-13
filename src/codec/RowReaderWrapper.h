/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_ROWREADERWRAPPER_H_
#define CODEC_ROWREADERWRAPPER_H_

#include <gtest/gtest_prod.h>

#include "codec/RowReaderV1.h"
#include "codec/RowReaderV2.h"
#include "common/base/Base.h"

namespace nebula {

/**
 * @brief A wrapper class to hide details of RowReaderV1 and RowReaderV2
 */
class RowReaderWrapper : public RowReader {
  FRIEND_TEST(RowReaderV1, headerInfo);
  FRIEND_TEST(RowReaderV1, encodedData);
  FRIEND_TEST(RowReaderV2, encodedData);

 public:
  RowReaderWrapper() = default;

  RowReaderWrapper(const RowReaderWrapper&) = delete;

  RowReaderWrapper& operator=(const RowReaderWrapper&) = delete;

  /**
   * @brief Move constructor of row reader wrapper
   *
   * @param rhs
   */
  RowReaderWrapper(RowReaderWrapper&& rhs) {
    this->readerVer_ = rhs.readerVer_;
    if (this->readerVer_ == 1) {
      this->readerV1_ = std::move(rhs.readerV1_);
      this->currReader_ = &(this->readerV1_);
    } else if (this->readerVer_ == 2) {
      this->readerV2_ = std::move(rhs.readerV2_);
      this->currReader_ = &(this->readerV2_);
    } else {
      this->currReader_ = nullptr;
    }
  }

  /**
   * @brief Move assign operator
   *
   * @param rhs
   * @return RowReaderWrapper&
   */
  RowReaderWrapper& operator=(RowReaderWrapper&& rhs) {
    this->readerVer_ = rhs.readerVer_;
    if (this->readerVer_ == 1) {
      this->readerV1_ = std::move(rhs.readerV1_);
      this->currReader_ = &(this->readerV1_);
    } else if (this->readerVer_ == 2) {
      this->readerV2_ = std::move(rhs.readerV2_);
      this->currReader_ = &(this->readerV2_);
    } else {
      this->currReader_ = nullptr;
    }
    return *this;
  }

  /**
   * @brief Generate a row reader wrapper of tag data
   *
   * @param schemaMan Schema manager, used to find the related schema of data
   * @param space SpaceId
   * @param tag TagId
   * @param row Value in kv engine
   * @return RowReaderWrapper The row reader wrapper
   */
  static RowReaderWrapper getTagPropReader(meta::SchemaManager* schemaMan,
                                           GraphSpaceID space,
                                           TagID tag,
                                           folly::StringPiece row);

  /**
   * @brief Generate a row reader wrapper of edge data
   *
   * @param schemaMan Schema manager, used to find the related schema of data
   * @param space SpaceId
   * @param tag TagId
   * @param row Value in kv engine
   * @return RowReaderWrapper The row reader wrapper
   */
  static RowReaderWrapper getEdgePropReader(meta::SchemaManager* schemaMan,
                                            GraphSpaceID space,
                                            EdgeType edge,
                                            folly::StringPiece row);

  /**
   * @brief Generate a row reader wrapper of data
   *
   * @param schema
   * @param row
   * @return RowReaderWrapper
   */
  static RowReaderWrapper getRowReader(meta::SchemaProviderIf const* schema,
                                       folly::StringPiece row);

  /**
   * @brief Generate a row reader wrapper of data, the schemas are stored in vector.
   * notice: the schemas are from oldest to newest,
   * usually from getAllVerTagSchema or getAllVerEdgeSchema in SchemaMan
   *
   * @param schemas
   * @param row
   * @return RowReaderWrapper
   */
  static RowReaderWrapper getRowReader(
      const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
      folly::StringPiece row);

  /**
   * @brief Construct a new row reader wrapper
   *
   * @param schema
   * @param row
   * @param readerVer Row reader version
   */
  RowReaderWrapper(const meta::SchemaProviderIf* schema,
                   const folly::StringPiece& row,
                   int32_t& readerVer);

  /**
   * @brief Reset current row reader wrapper to of given schema, data and reader version
   *
   * @param schema
   * @param row
   * @param readVer
   * @return Whether reset succeed
   */
  bool reset(meta::SchemaProviderIf const* schema, folly::StringPiece row, int32_t readVer);

  /**
   * @brief Reset current row reader wrapper to of given schema and data
   *
   * @param schema
   * @param row
   * @return Whether reset succeed
   */
  bool reset(meta::SchemaProviderIf const* schema, folly::StringPiece row);

  /**
   * @brief Reset current row reader wrapper of given schemas and data, the schemas are stored in
   * vector.
   *
   * @param schemas
   * @param row
   * @return Whether reset succeed
   */
  bool reset(const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
             folly::StringPiece row);

  Value getValueByName(const std::string& prop) const override {
    DCHECK(!!currReader_);
    return currReader_->getValueByName(prop);
  }

  Value getValueByIndex(const int64_t index) const override {
    DCHECK(!!currReader_);
    return currReader_->getValueByIndex(index);
  }

  int64_t getTimestamp() const noexcept override {
    DCHECK(!!currReader_);
    return currReader_->getTimestamp();
  }

  int32_t readerVer() const noexcept override {
    DCHECK(!!currReader_);
    return currReader_->readerVer();
  }

  // Return the number of bytes used for the header info
  size_t headerLen() const noexcept override {
    DCHECK(!!currReader_);
    return currReader_->headerLen();
  }

  Iterator begin() const override {
    DCHECK(!!currReader_);
    return currReader_->begin();
  }

  const Iterator& end() const override {
    DCHECK(!!currReader_);
    return currReader_->end();
  }

  SchemaVer schemaVer() const noexcept override {
    DCHECK(!!currReader_);
    return currReader_->schemaVer();
  }

  size_t numFields() const noexcept override {
    DCHECK(!!currReader_);
    return currReader_->numFields();
  }

  const meta::SchemaProviderIf* getSchema() const override {
    DCHECK(!!currReader_);
    return currReader_->getSchema();
  }

  const std::string getData() const override {
    DCHECK(!!currReader_);
    return currReader_->getData();
  }

  /**
   * @brief Get schema version and reader version by data
   *
   * @param row Row data
   * @param schemaVer Schema version
   * @param readerVer Row reader version
   */
  static void getVersions(const folly::StringPiece& row, SchemaVer& schemaVer, int32_t& readerVer);

  /**
   * @brief Return whether wrapper points to a valid data
   */
  operator bool() const noexcept {
    return operator!=(nullptr);
  }

  /**
   * @brief Return whether wrapper points to a valid data
   */
  bool operator==(std::nullptr_t) const noexcept {
    return !operator!=(nullptr);
  }

  /**
   * @brief Return whether wrapper points to a valid data
   */
  bool operator!=(std::nullptr_t) const noexcept {
    return currReader_ != nullptr;
  }

  /**
   * @brief Return this row reader wrapper
   */
  RowReaderWrapper* operator->() const noexcept {
    return get();
  }

  /**
   * @brief Return this row reader wrapper
   */
  RowReaderWrapper* get() const noexcept {
    return const_cast<RowReaderWrapper*>(this);
  }

  /**
   * @brief Return this row reader wrapper
   */
  RowReaderWrapper* get() noexcept {
    return this;
  }

  /**
   * @brief Return this row reader wrapper
   */
  RowReaderWrapper& operator*() const noexcept {
    return *get();
  }

  /**
   * @brief Reset to an empty row reader
   */
  void reset() noexcept {
    currReader_ = nullptr;
  }

 private:
  RowReaderV1 readerV1_;
  RowReaderV2 readerV2_;
  RowReader* currReader_ = nullptr;
  int32_t readerVer_ = 0;
};

}  // namespace nebula
#endif  // CODEC_ROWREADERWRAPPER_H_
