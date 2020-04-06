/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADERWRAPPER_H_
#define CODEC_ROWREADERWRAPPER_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "codec/RowReaderV1.h"
#include "codec/RowReaderV2.h"

namespace nebula {

class RowReaderWrapper : public RowReader {
    FRIEND_TEST(RowReaderV1, headerInfo);
    FRIEND_TEST(RowReaderV1, encodedData);
    FRIEND_TEST(RowReaderV2, encodedData);

public:
    bool reset(meta::SchemaProviderIf const* schema,
               folly::StringPiece row) noexcept override;

    Value getValueByName(const std::string& prop) const noexcept override {
        DCHECK(!!currReader_);
        return currReader_->getValueByName(prop);
    }

    Value getValueByIndex(const int64_t index) const noexcept override {
        DCHECK(!!currReader_);
        return currReader_->getValueByIndex(index);
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

private:
    RowReaderV1 readerV1_;
    RowReaderV2 readerV2_;
    RowReader* currReader_;

    static void getVersions(const folly::StringPiece& row,
                            SchemaVer& schemaVer,
                            int32_t& readerVer);
};

}  // namespace nebula
#endif  // CODEC_ROWREADERWRAPPER_H_

