/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_ROWWRITER_H_
#define DATAMAN_ROWWRITER_H_

#include "base/Base.h"
#include "base/ICord.h"
#include "dataman/SchemaWriter.h"

namespace nebula {

/**
 * It's a write-only data streamer, used to encode one row of data
 *
 * It can be used with or without schema. When no schema is assigned,
 * a new schema will be created according to the input data stream
 */
class RowWriter {
public:
    /*******************
     *
     * Stream Control
     *
     ******************/
    // Set the name for the next column
    // This cannot be used if a schema is provided
    struct ColName {
        friend class RowWriter;
        explicit ColName(std::string&& name) : name_(std::move(name)) {}
        explicit ColName(const folly::StringPiece name)
            : name_(name.begin(), name.size()) {}
        explicit ColName(const char* name) : name_(name) {}
        ColName(ColName&& rhs) : name_(std::move(rhs.name_)) {}
    private:
        std::string name_;
    };

    // Set the type for the next column
    // This cannot be used if a schema is provided
    struct ColType {
        friend class RowWriter;
        template<class VType>
        explicit ColType(VType&& type) : type_(std::forward(type)) {}
        explicit ColType(cpp2::SupportedType type) {
            type_.set_type(type);
        }
        ColType(ColType&& rhs) : type_(std::move(rhs.type_)) {}
    private:
        cpp2::ValueType type_;
    };

    // Skip next few columns. Default values will be written for those
    // fields
    // This cannot be used if a schema is not provided
    struct Skip {
        friend class RowWriter;
        explicit Skip(int64_t toSkip) : toSkip_(toSkip) {}
    private:
        int64_t toSkip_;
    };

public:
    explicit RowWriter(
        std::shared_ptr<const meta::SchemaProviderIf> schema
            = std::shared_ptr<const meta::SchemaProviderIf>());

    // Encode into a binary array
    std::string encode() noexcept;
    // Encode and attach to the given string
    // For the sake of performance, th caller needs to make sure the sting
    // is large enough so that resize will not happen
    void encodeTo(std::string& encoded) noexcept;

    // Calculate the exact length of the encoded binary array
    int64_t size() const noexcept;

    std::shared_ptr<const meta::SchemaProviderIf> schema() const {
        return schema_;
    }

    // Move the schema out of the writer
    // After the schema being moved, **NO MORE** write should happen
    cpp2::Schema moveSchema();

    // Data stream
    RowWriter& operator<<(bool v) noexcept;
    RowWriter& operator<<(float v) noexcept;
    RowWriter& operator<<(double v) noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, RowWriter&>::type
    operator<<(T v) noexcept;

    RowWriter& operator<<(const std::string& v) noexcept;
    RowWriter& operator<<(folly::StringPiece v) noexcept;
    RowWriter& operator<<(const char* v) noexcept;

    // Control stream
    RowWriter& operator<<(ColName&& colName) noexcept;
    RowWriter& operator<<(ColType&& colType) noexcept;
    RowWriter& operator<<(Skip&& skip) noexcept;

private:
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::shared_ptr<SchemaWriter> schemaWriter_;
    ICord<> cord_;

    int64_t colNum_ = 0;
    std::unique_ptr<ColName> colName_;
    std::unique_ptr<ColType> colType_;

    // Block offsets for every 16 fields
    std::vector<int64_t> blockOffsets_;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value>::type
    writeInt(T v);

    // Calculate the number of bytes occupied (ignore the leading 0s)
    int64_t calcOccupiedBytes(uint64_t v) const noexcept;
};

}  // namespace nebula


#define RW_GET_COLUMN_TYPE(STYPE) \
    const cpp2::ValueType* type; \
    if (colNum_ >= static_cast<int64_t>(schema_->getNumFields())) { \
        CHECK(!!schemaWriter_) << "SchemaWriter cannot be NULL"; \
        if (!colType_) { \
            colType_.reset(new ColType(cpp2::SupportedType::STYPE)); \
        } \
        type = &(colType_->type_); \
    } else { \
        type = &(schema_->getFieldType(colNum_)); \
    }


#define RW_CLEAN_UP_WRITE() \
    colNum_++; \
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) { \
        /* We need to record offset for every 16 fields */ \
        blockOffsets_.emplace_back(cord_.size()); \
    } \
    if (colNum_ > static_cast<int64_t>(schema_->getNumFields())) { \
        /* Need to append the new column type to the schema */ \
        if (!colName_) { \
            schemaWriter_->appendCol(folly::stringPrintf("Column%ld", colNum_), \
                                     std::move(colType_->type_)); \
        } else { \
            schemaWriter_->appendCol(std::move(colName_->name_), \
                                     std::move(colType_->type_)); \
        } \
    } \
    colName_.reset(); \
    colType_.reset();


#include "dataman/RowWriter.inl"

#endif  // DATAMAN_ROWWRITER_H_



