/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADER_H_
#define CODEC_ROWREADER_H_

#include "base/Base.h"
#include "datatypes/Value.h"
#include "codec/Common.h"
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"

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
        Iterator(Iterator&& iter);

        void operator=(Iterator&& rhs);

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
    static std::unique_ptr<RowReader> getTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        folly::StringPiece row);

    static std::unique_ptr<RowReader> getEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        folly::StringPiece row);

    static std::unique_ptr<RowReader> getRowReader(
        meta::SchemaProviderIf const* schema,
        folly::StringPiece row);

    // notice: the schemas are from oldest to newest,
    // usually from getAllVerTagSchema or getAllVerEdgeSchema in SchemaMan
    static std::unique_ptr<RowReader> getRowReader(
        const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
        folly::StringPiece row);

    bool resetTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        folly::StringPiece row);

    bool resetEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        folly::StringPiece row);

    bool reset(meta::SchemaProviderIf const* schema,
               folly::StringPiece row) noexcept;

    // notice: the schemas are from oldest to newest,
    // usually from getAllVerTagSchema or getAllVerEdgeSchema in SchemaMan
    bool reset(const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
               folly::StringPiece row) noexcept;

    virtual ~RowReader() = default;

    virtual Value getValueByName(const std::string& prop) const noexcept = 0;
    virtual Value getValueByIndex(const int64_t index) const noexcept = 0;

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

    virtual bool reset(meta::SchemaProviderIf const* schema,
                       folly::StringPiece row,
                       int32_t readerVer) noexcept = 0;

private:
    Iterator endIter_;
};

}  // namespace nebula
#endif  // CODEC_ROWREADER_H_
