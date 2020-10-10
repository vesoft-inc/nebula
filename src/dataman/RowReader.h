/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_ROWREADER_H_
#define DATAMAN_ROWREADER_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "gen-cpp2/graph_types.h"
#include "interface/gen-cpp2/common_types.h"
#include "dataman/DataCommon.h"
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "base/ErrorOr.h"

namespace nebula {

/**
 * This class decodes one row of data
 */
class RowReader {
    FRIEND_TEST(RowReader, headerInfo);
    FRIEND_TEST(RowReader, encodedData);
    FRIEND_TEST(RowWriter, offsetsCreation);

public:
    class Iterator;

    class Cell final {
        friend class Iterator;
    public:
        template<typename T>
        typename std::enable_if<std::is_integral<T>::value, ResultType>::type
            getInt(T& v) const noexcept;
        ResultType getBool(bool& v) const noexcept;
        ResultType getFloat(float& v) const noexcept;
        ResultType getDouble(double& v) const noexcept;
        ResultType getString(folly::StringPiece& v) const noexcept;
        ResultType getVid(int64_t& v) const noexcept;
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
    RowReader(const RowReader&) = delete;
    RowReader& operator=(const RowReader&) = delete;
    RowReader(RowReader&& x) = default;
    RowReader& operator=(RowReader&& x) = default;

    static RowReader getTagPropReader(
        meta::SchemaManager* schemaMan,
        folly::StringPiece row,
        GraphSpaceID space,
        TagID tag);

    static RowReader getEdgePropReader(
        meta::SchemaManager* schemaMan,
        folly::StringPiece row,
        GraphSpaceID space,
        EdgeType edge);

    static RowReader getRowReader(
        folly::StringPiece row,
        std::shared_ptr<const meta::SchemaProviderIf> schema);

    static RowReader getEmptyRowReader() {
        return RowReader();
    }

    static StatusOr<VariantType> getDefaultProp(const meta::SchemaProviderIf* schema,
                                                const std::string& prop) {
        auto& vType = schema->getFieldType(prop);
        auto defaultVal = getDefaultProp(vType.type);
        if (!defaultVal.ok()) {
            LOG(ERROR) << "Get default value for `" << prop << "' failed: " << defaultVal.status();
        }

        return defaultVal;
    }

    static StatusOr<VariantType> getDefaultProp(const nebula::cpp2::SupportedType& type) {
        switch (type) {
            case nebula::cpp2::SupportedType::BOOL: {
                return false;
            }
            case nebula::cpp2::SupportedType::TIMESTAMP:
            case nebula::cpp2::SupportedType::INT:
                return static_cast<int64_t>(0);
            case nebula::cpp2::SupportedType::VID: {
                return static_cast<VertexID>(0);
            }
            case nebula::cpp2::SupportedType::FLOAT:
            case nebula::cpp2::SupportedType::DOUBLE: {
                return static_cast<double>(0.0);
            }
            case nebula::cpp2::SupportedType::STRING: {
                return static_cast<std::string>("");
            }
            default:
                auto msg = folly::sformat("Unknown type: {}", static_cast<int32_t>(type));
                LOG(ERROR) << msg;
                return Status::Error(msg);
        }
    }

    static ErrorOr<ResultType, VariantType> getPropByName(const RowReader* reader,
                                                          const std::string& prop) {
        auto& vType = reader->getSchema()->getFieldType(prop);
        switch (vType.type) {
            case nebula::cpp2::SupportedType::BOOL: {
                bool v;
                auto ret = reader->getBool(prop, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::INT:
            case nebula::cpp2::SupportedType::TIMESTAMP: {
                int64_t v;
                auto ret = reader->getInt(prop, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::VID: {
                VertexID v;
                auto ret = reader->getVid(prop, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::FLOAT: {
                float v;
                auto ret = reader->getFloat(prop, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return static_cast<double>(v);
            }
            case nebula::cpp2::SupportedType::DOUBLE: {
                double v;
                auto ret = reader->getDouble(prop, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::STRING: {
                folly::StringPiece v;
                auto ret = reader->getString(prop, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v.toString();
            }
            default:
                VLOG(2) << "Unknown type: " << static_cast<int32_t>(vType.type);
                return ResultType::E_DATA_INVALID;
        }
    }


    static ErrorOr<ResultType, VariantType> getPropByIndex(const RowReader *reader,
                                                           const int64_t index) {
        auto& vType = reader->getSchema()->getFieldType(index);
        switch (vType.get_type()) {
            case nebula::cpp2::SupportedType::BOOL: {
                bool v;
                auto ret = reader->getBool(index, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::INT:
            case nebula::cpp2::SupportedType::TIMESTAMP: {
                int64_t v;
                auto ret = reader->getInt(index, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::VID: {
                VertexID v;
                auto ret = reader->getVid(index, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::FLOAT: {
                float v;
                auto ret = reader->getFloat(index, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return static_cast<double>(v);
            }
            case nebula::cpp2::SupportedType::DOUBLE: {
                double v;
                auto ret = reader->getDouble(index, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v;
            }
            case nebula::cpp2::SupportedType::STRING: {
                folly::StringPiece v;
                auto ret = reader->getString(index, v);
                if (ret != ResultType::SUCCEEDED) {
                    return ret;
                }
                return v.toString();
            }
            default:
                VLOG(2) << "Unknown type: " << static_cast<int32_t>(vType.get_type());
                return ResultType::E_DATA_INVALID;
        }
    }
    SchemaVer schemaVer() const noexcept;
    int32_t numFields() const noexcept;

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

    ResultType getBool(const folly::StringPiece name, bool& v) const noexcept;
    ResultType getBool(int64_t index, bool& v) const noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    getInt(const folly::StringPiece name, T& v) const noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
    getInt(int64_t index, T& v) const noexcept;

    ResultType getFloat(const folly::StringPiece name, float& v) const noexcept;
    ResultType getFloat(int64_t index, float& v) const noexcept;

    ResultType getDouble(const folly::StringPiece name, double& v) const noexcept;
    ResultType getDouble(int64_t index, double& v) const noexcept;

    ResultType getString(const folly::StringPiece name,
                         folly::StringPiece& v) const noexcept;
    ResultType getString(int64_t index,
                         folly::StringPiece& v) const noexcept;

    ResultType getVid(const folly::StringPiece name, int64_t& v) const noexcept;
    ResultType getVid(int64_t index, int64_t& v) const noexcept;

    std::shared_ptr<const meta::SchemaProviderIf> getSchema() const {
        return schema_;
    }

    static int32_t getSchemaVer(folly::StringPiece row);

    folly::StringPiece getData() const noexcept {
        return data_;
    }

    operator bool() const noexcept {
        return operator!=(nullptr);
    }

    bool operator==(std::nullptr_t) const noexcept {
        return !data_.data();
    }

    bool operator==(const RowReader& x) const noexcept {
        return data_ == x.data_;
    }

    bool operator!=(std::nullptr_t) const noexcept {
        return static_cast<bool>(data_.data());
    }

    bool operator!=(const RowReader& x) const noexcept {
        return data_ != x.data_;
    }

    RowReader* operator->() const noexcept {
        return get();
    }

    RowReader* get() const noexcept {
        return const_cast<RowReader*>(this);
    }

    RowReader* get() noexcept {
        return this;
    }

    RowReader& operator*() const noexcept {
        return *get();
    }

    void reset() noexcept {
        this->~RowReader();
        new(this) RowReader();
    }

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

private:
    RowReader() = default;
    RowReader(folly::StringPiece row,
              std::shared_ptr<const meta::SchemaProviderIf> schema);

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

    // The following methods all return the number of bytes read
    // A negative number will be returned if an error occurs
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, int32_t>::type
        readInteger(int64_t offset, T& v) const noexcept;
    int32_t readFloat(int64_t offset, float& v) const noexcept;
    int32_t readDouble(int64_t offset, double& v) const noexcept;
    int32_t readString(int64_t offset, folly::StringPiece& v) const noexcept;
    int32_t readInt64(int64_t offset, int64_t& v) const noexcept;
    int32_t readVid(int64_t offset, int64_t& v) const noexcept;

    // Following methods assume the parameters index and offset are valid
    // When succeeded, offset will advance
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, ResultType>::type
        getInt(int64_t index, int64_t& offset, T& v) const noexcept;
    ResultType getBool(int64_t index, int64_t& offset, bool& v) const noexcept;
    ResultType getFloat(int64_t index, int64_t& offset, float& v) const noexcept;
    ResultType getDouble(int64_t index, int64_t& offset, double& v)
        const noexcept;
    ResultType getString(int64_t index, int64_t& offset, folly::StringPiece& v)
        const noexcept;
    ResultType getInt64(int64_t index, int64_t& offset, int64_t& v) const noexcept;
    ResultType getVid(int64_t index, int64_t& offset, int64_t& v) const noexcept;
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

#define RR_GET_VALUE_BY_NAME(FN, VT) \
    RowReader::get ## FN(const folly::StringPiece name, \
                          VT& v) const noexcept { \
        int64_t index = schema_->getFieldIndex(name); \
        if (index < 0) { \
            return ResultType::E_NAME_NOT_FOUND; \
        } else { \
            return get ## FN (index, v); \
        } \
    }

#define RR_GET_OFFSET() \
    int64_t offset = skipToField(index); \
    if (offset < 0) { \
        return static_cast<ResultType>(offset); \
    } \
    if (index >= static_cast<int64_t>(schema_->getNumFields())) { \
        return ResultType::E_INDEX_OUT_OF_RANGE; \
    }


#include "dataman/RowReader.inl"

#endif  // DATAMAN_ROWREADER_H_
