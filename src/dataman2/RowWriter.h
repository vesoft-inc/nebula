/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN2_ROWWRITER_H_
#define DATAMAN2_ROWWRITER_H_

#include "base/Base.h"
#include "base/Cord.h"
#include "base/NullValue.h"
#include "dataman2/SchemaWriter.h"

namespace nebula {

class ValueRequiredException : public std::exception {
};


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

    // Skip next few columns.
    struct Skip {
        friend class RowWriter;
        explicit Skip(int64_t numColsToSkip) : numColsToSkip_(numColsToSkip) {}
    private:
        int64_t numColsToSkip_;
    };

public:
    explicit RowWriter(
        std::shared_ptr<const meta::SchemaProviderIf> schema
            = std::shared_ptr<const meta::SchemaProviderIf>());

    // Encode into a binary array
    std::string encode();
    // Encode and attach to the given string
    // For the sake of performance, th caller needs to make sure the sting
    // is large enough so that resize will not happen
    void encodeTo(std::string& encoded);

    // Calculate the exact length of the encoded binary array
    int64_t size() noexcept;

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

    RowWriter& operator<<(int64_t v) noexcept;
    template<typename T, typename = std::enable_if_t<std::is_integral<T>::value>>
    RowWriter& operator<<(T v) noexcept {
        return operator<<(static_cast<int64_t>(v));
    }

    RowWriter& operator<<(const std::string& v) noexcept;
    RowWriter& operator<<(folly::StringPiece v) noexcept;
    RowWriter& operator<<(const char* v) noexcept;

    RowWriter& operator<<(NullValue::NullType type) noexcept;

    // Control stream
    RowWriter& operator<<(ColName&& colName) noexcept;
    RowWriter& operator<<(ColType&& colType) noexcept;
    // Skip next several fields
    // Null values or default values will be written for those fields
    // depending on the schema
    //
    // If the fields are non-nullable and no default value is provided,
    // the method will throw
    RowWriter& operator<<(Skip&& skip);

private:
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::shared_ptr<SchemaWriter> schemaWriter_;
    Cord cord_;

    int64_t colNum_ = 0;
    std::unique_ptr<ColName> colName_;
    std::unique_ptr<ColType> colType_;

    // Block offsets for every 16 fields
    std::vector<int64_t> blockOffsets_;
    // Null flags
    // When the bit is set, the corresponding field is null, the field will occupy
    // one byte to indicate the reason
    std::vector<uint8_t> nullFlags_;

    // Calculate the number of bytes occupied (ignore the leading 0s
    int64_t calcOccupiedBytes(uint64_t v) const noexcept;

    // Return the column type
    // If it's a new column, append the column definition to the SchemaWriter
    const cpp2::ValueType* getColumnType(int64_t colIndex,
                                         cpp2::SupportedType valueType) noexcept;

    // Output an integral value
    void writeInt(int64_t v);

    // Output the Null value
    void writeNullValue(int64_t colIndex, NullValue::NullType type) noexcept;

    void finishWrite() noexcept;
};

}  // namespace nebula
#endif  // DATAMAN2_ROWWRITER_H_



