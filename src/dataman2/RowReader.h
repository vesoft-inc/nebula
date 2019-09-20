/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN2_ROWREADER_H_
#define DATAMAN2_ROWREADER_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "gen-cpp2/graph_types.h"
#include "gen-cpp2/common_types.h"
#include "dataman2/DataCommon.h"
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "base/ErrorOr.h"

namespace nebula {

/**
 * This class decodes one row of data
 */
class RowReader {
    FRIEND_TEST(RowReader2, headerInfo);
    FRIEND_TEST(RowReader2, encodedData);
    FRIEND_TEST(RowWriter2, offsetsCreation);

public:
    class Iterator;

    class Cell final {
        friend class Iterator;
    public:
        ResultType getInt(EitherOr<NullValue, int64_t>& v) const noexcept;
        template<typename T, typename = std::enable_if_t<std::is_integral<T>::value>>
        ResultType getInt(EitherOr<NullValue, T>& v) const noexcept {
            EitherOr<NullValue, int64_t> iVal;
            auto res = getInt(iVal);
            if (res == ResultType::SUCCEEDED) {
                if (iVal.isLeftType()) {
                    v = iVal.left();
                } else {
                    v = static_cast<T>(iVal.right());
                }
            }
            return res;
        }
        ResultType getBool(EitherOr<NullValue, bool>& v) const noexcept;
        ResultType getFloat(EitherOr<NullValue, float>& v) const noexcept;
        ResultType getDouble(EitherOr<NullValue, double>& v) const noexcept;
        ResultType getString(EitherOr<NullValue, folly::StringPiece>& v) const noexcept;
        ResultType getVid(EitherOr<NullValue, int64_t>& v) const noexcept;

    private:
        const RowReader* reader_;
        Iterator* iter_;

        Cell(const RowReader* reader, Iterator* iter)
            : reader_(reader), iter_(iter) {}
    };
    friend class Cell;


    class Iterator final {
        friend class RowReader;
    public:
        Iterator(Iterator&& iter);
        const Cell& operator*() const;
        const Cell* operator->() const;
        Iterator& operator++();
        operator bool() const;
        bool operator==(const Iterator& rhs) const noexcept;

    private:
        const RowReader* reader_;
        const size_t numFields_;
        std::unique_ptr<Cell> cell_;
        int64_t index_ = 0;
        int32_t bytes_ = 0;
        int64_t offset_ = 0;

        Iterator(const RowReader* reader, size_t numFields, int64_t index = 0);
    };


public:
    RowReader(folly::StringPiece row,
              std::shared_ptr<const meta::SchemaProviderIf> schema);
    virtual ~RowReader() = default;

    static std::unique_ptr<RowReader> getTagPropReader(
        meta::SchemaManager* schemaMan,
        folly::StringPiece row,
        GraphSpaceID space,
        TagID tag);

    static std::unique_ptr<RowReader> getEdgePropReader(
        meta::SchemaManager* schemaMan,
        folly::StringPiece row,
        GraphSpaceID space,
        EdgeType edge);

    ErrorOr<ResultType, VariantType> getPropByName(const std::string& prop);
    ErrorOr<ResultType, VariantType> getPropByIndex(const int64_t index);

    bool isNull(const std::string& prop) const noexcept;
    bool isNull(const int64_t index) const noexcept;

    SchemaVer schemaVer() const noexcept {
        return schema_->getVersion();
    }

    int32_t numFields() const noexcept {
        return schema_->getNumFields();
    }

    std::shared_ptr<const meta::SchemaProviderIf> getSchema() const {
        return schema_;
    }

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

    ResultType getBool(const folly::StringPiece name, EitherOr<NullValue, bool>& v)
        const noexcept;
    ResultType getBool(int64_t index, EitherOr<NullValue, bool>& v)
        const noexcept;

    ResultType getInt(const folly::StringPiece name, EitherOr<NullValue, int64_t>& v)
        const noexcept;
    ResultType getInt(int64_t index, EitherOr<NullValue, int64_t>& v)
        const noexcept;
    template<typename T, typename = std::enable_if_t<std::is_integral<T>::value>>
    ResultType getInt(const folly::StringPiece name, EitherOr<NullValue, T>& v)
            const noexcept {
        EitherOr<NullValue, int64_t> iVal;
        auto res = getInt(name, iVal);
        if (res == ResultType::SUCCEEDED) {
            if (iVal.isLeftType()) {
                v = iVal.left();
            } else {
                v = static_cast<T>(iVal.right());
            }
        }
        return res;
    }
    template<typename T, typename = std::enable_if_t<std::is_integral<T>::value>>
    ResultType getInt(int64_t index, EitherOr<NullValue, T>& v) const noexcept {
        EitherOr<NullValue, int64_t> iVal;
        auto res = getInt(index, iVal);
        if (res == ResultType::SUCCEEDED) {
            if (iVal.isLeftType()) {
                v = iVal.left();
            } else {
                v = static_cast<T>(iVal.right());
            }
        }
        return res;
    }

    ResultType getFloat(const folly::StringPiece name, EitherOr<NullValue, float>& v)
        const noexcept;
    ResultType getFloat(int64_t index, EitherOr<NullValue, float>& v)
        const noexcept;

    ResultType getDouble(const folly::StringPiece name, EitherOr<NullValue, double>& v)
        const noexcept;
    ResultType getDouble(int64_t index, EitherOr<NullValue, double>& v)
        const noexcept;

    ResultType getString(const folly::StringPiece name,
                         EitherOr<NullValue, folly::StringPiece>& v) const noexcept;
    ResultType getString(int64_t index,
                         EitherOr<NullValue, folly::StringPiece>& v) const noexcept;

    ResultType getVid(const folly::StringPiece name, EitherOr<NullValue, int64_t>& v)
        const noexcept;
    ResultType getVid(int64_t index, EitherOr<NullValue, int64_t>& v)
        const noexcept;

    // TODO getPath(const std::string& name) const noexcept;
    // TODO getPath(int64_t index) const noexcept;
    // TODO getList(const std::string& name) const noexcept;
    // TODO getList(int64_t index) const noexcept;
    // TODO getSet(const std::string& name) const noexcept;
    // TODO getSet(int64_t index) const noexcept;
    // TODO getMap(const std::string& name) const noexcept;
    // TODO getMap(int64_t index) const noexcept;

private:
    std::shared_ptr<const meta::SchemaProviderIf> schema_;

    folly::StringPiece data_;
    int32_t headerLen_ = 0;
    int32_t numBytesForOffset_ = 0;
    // Block offet value is composed by two integers. The first one is
    // the block offset, the second one is the largest index being visited
    // in the block. This index is zero-based
    mutable std::vector<std::pair<int64_t, uint8_t>> blockOffsets_;
    mutable std::vector<int64_t> offsets_;

    std::vector<uint8_t> nullFlags_;

private:
    static int32_t getSchemaVer(folly::StringPiece row);

    // Process the row header infomation
    // Returns false when the row data is invalid
    bool processHeader(folly::StringPiece row);

    // Process the block offsets (each block contains certain number of fields)
    // Returns false when the row data is invalid
    bool processBlockOffsets(folly::StringPiece row, int32_t verBytes);

    // Skip to the next field
    // Parameter:
    //  index   : the current field index
    //  offset  : the current offset
    // When succeeded, the method returns the offset pointing to the
    // next field
    // When failed, the method returns a negative number
    int64_t skipToNext(int64_t index, int64_t offset) const noexcept;

    // Skip to the {index}Th field
    // The method retuns the offset of the field
    // It returns a negative number when the data corrupts
    int64_t skipToField(int64_t index) const noexcept;

    int64_t getOffset(int64_t index) const noexcept;

    // The following methods all return the number of bytes read
    // A negative number will be returned if an error occurs
    int32_t readInteger(int64_t offset, int64_t& v) const noexcept;
    int32_t readFloat(int64_t offset, float& v) const noexcept;
    int32_t readDouble(int64_t offset, double& v) const noexcept;
    int32_t readString(int64_t offset, folly::StringPiece& v) const noexcept;
    int32_t readInt64(int64_t offset, int64_t& v) const noexcept;
    int32_t readVid(int64_t offset, int64_t& v) const noexcept;

    // Following methods assume the parameters index and offset are valid
    // When succeeded, offset will advance
    ResultType getInt(int64_t index, int64_t& offset, EitherOr<NullValue, int64_t>& v)
        const noexcept;
    ResultType getBool(int64_t index, int64_t& offset, EitherOr<NullValue, bool>& v)
        const noexcept;
    ResultType getFloat(int64_t index, int64_t& offset, EitherOr<NullValue, float>& v)
        const noexcept;
    ResultType getDouble(int64_t index, int64_t& offset, EitherOr<NullValue, double>& v)
        const noexcept;
    ResultType getString(int64_t index,
                         int64_t& offset,
                         EitherOr<NullValue, folly::StringPiece>& v) const noexcept;
    ResultType getInt64(int64_t index, int64_t& offset, EitherOr<NullValue, int64_t>& v)
        const noexcept;
    ResultType getVid(int64_t index, int64_t& offset, EitherOr<NullValue, int64_t>& v)
        const noexcept;
};

}  // namespace nebula


#define RR_CELL_GET_VALUE(FN) \
    if (!*iter_) { \
        /* Already reached the end, or an error happened */ \
        return ResultType::E_INDEX_OUT_OF_RANGE; \
    } \
    int64_t offset = iter_->offset_; \
    ResultType res = reader_->get ## FN(iter_->index_, offset, v); \
    if (res == ResultType::SUCCEEDED) { \
        iter_->bytes_ = offset - iter_->offset_; \
    } else { \
        /* Move iterator to the end */ \
        iter_->index_ = iter_->numFields_; \
    } \
    return res

#endif  // DATAMAN2_ROWREADER_H_
