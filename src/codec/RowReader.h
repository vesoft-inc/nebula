/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADER_H_
#define CODEC_ROWREADER_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "common/meta/SchemaProviderIf.h"
#include "common/meta/SchemaManager.h"
#include "codec/Common.h"

namespace nebula {

/**
 * This class decodes one row of data
 */
class RowReader {
public:
    class Iterator;

    class Cell final {
        friend class Iterator;
    public:
        Value value() const noexcept;

    private:
        const Iterator* iter_;

        explicit Cell(const Iterator* iter) : iter_(iter) {}
    };


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

        explicit Iterator(const RowReader* reader)
            : reader_(reader), cell_(this) {}

        Iterator(const RowReader* reader, size_t index)
            : reader_(reader), cell_(this), index_(index) {}

        void reset(size_t index = 0) {
            index_ = index;
        }
    };


public:
    virtual ~RowReader() = default;

    virtual Value getValueByName(const std::string& prop) const noexcept = 0;
    virtual Value getValueByIndex(const int64_t index) const noexcept = 0;
    virtual int64_t getTimestamp() const noexcept = 0;

    virtual int32_t readerVer() const noexcept = 0;

    // Return the number of bytes used for the header info
    virtual size_t headerLen() const noexcept = 0;


    virtual Iterator begin() const noexcept {
        return Iterator(this, 0);
    }

    virtual const Iterator& end() const noexcept {
        return endIter_;
    }

    virtual SchemaVer schemaVer() const noexcept {
        return schema_->getVersion();
    }

    virtual size_t numFields() const noexcept {
        return schema_->getNumFields();
    }

    virtual const meta::SchemaProviderIf* getSchema() const {
        return schema_;
    }

    virtual const std::string getData() const {
        return data_.toString();
    }

protected:
    meta::SchemaProviderIf const* schema_;
    folly::StringPiece data_;

    RowReader() : endIter_(this) {}

    virtual bool resetImpl(meta::SchemaProviderIf const* schema,
                           folly::StringPiece row) noexcept;

private:
    Iterator endIter_;
};

}  // namespace nebula
#endif  // CODEC_ROWREADER_H_
