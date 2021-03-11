/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADERWRAPPER_H_
#define CODEC_ROWREADERWRAPPER_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "codec/RowReaderV1.h"
#include "codec/RowReaderV2.h"

namespace nebula {

class RowReaderWrapper : public RowReader {
    FRIEND_TEST(RowReaderV1, headerInfo);
    FRIEND_TEST(RowReaderV1, encodedData);
    FRIEND_TEST(RowReaderV2, encodedData);

public:
    RowReaderWrapper() = default;

    RowReaderWrapper(const RowReaderWrapper&) = delete;

    RowReaderWrapper& operator=(const RowReaderWrapper&) = delete;

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

    static RowReaderWrapper getTagPropReader(meta::SchemaManager* schemaMan,
                                             GraphSpaceID space,
                                             TagID tag,
                                             folly::StringPiece row);

    static RowReaderWrapper getEdgePropReader(meta::SchemaManager* schemaMan,
                                              GraphSpaceID space,
                                              EdgeType edge,
                                              folly::StringPiece row);

    static RowReaderWrapper getRowReader(meta::SchemaProviderIf const* schema,
                                         folly::StringPiece row);

    // notice: the schemas are from oldest to newest,
    // usually from getAllVerTagSchema or getAllVerEdgeSchema in SchemaMan
    static RowReaderWrapper getRowReader(
        const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
        folly::StringPiece row);

    RowReaderWrapper(const meta::SchemaProviderIf* schema,
                     const folly::StringPiece& row,
                     int32_t& readerVer);

    bool reset(meta::SchemaProviderIf const* schema,
               folly::StringPiece row,
               int32_t readVer) noexcept;

    bool reset(meta::SchemaProviderIf const* schema,
               folly::StringPiece row) noexcept;

    bool reset(const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
               folly::StringPiece row) noexcept;

    Value getValueByName(const std::string& prop) const noexcept override {
        DCHECK(!!currReader_);
        return currReader_->getValueByName(prop);
    }

    Value getValueByIndex(const int64_t index) const noexcept override {
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

    Iterator begin() const noexcept override {
        DCHECK(!!currReader_);
        return currReader_->begin();
    }

    const Iterator& end() const noexcept override {
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

    static void getVersions(const folly::StringPiece& row,
                            SchemaVer& schemaVer,
                            int32_t& readerVer);

    operator bool() const noexcept {
        return operator!=(nullptr);
    }

    bool operator==(std::nullptr_t) const noexcept {
        return !operator!=(nullptr);
    }

    bool operator!=(std::nullptr_t) const noexcept {
        return currReader_ != nullptr;
    }

    RowReaderWrapper* operator->() const noexcept {
        return get();
    }

    RowReaderWrapper* get() const noexcept {
        return const_cast<RowReaderWrapper*>(this);
    }

    RowReaderWrapper* get() noexcept {
        return this;
    }

    RowReaderWrapper& operator*() const noexcept {
        return *get();
    }

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

