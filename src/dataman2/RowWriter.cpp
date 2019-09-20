/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman2/RowWriter.h"

namespace nebula {

using cpp2::Schema;
using cpp2::SupportedType;
using meta::SchemaProviderIf;

RowWriter::RowWriter(std::shared_ptr<const SchemaProviderIf> schema)
        : schema_(std::move(schema)) {
    if (!schema_) {
        // Need to create a new schema
        schemaWriter_.reset(new SchemaWriter());
        schema_ = schemaWriter_;
    }
}


int64_t RowWriter::size() noexcept {
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    SchemaVer verBytes = 0;
    if (schema_->getVersion() > 0) {
        verBytes = calcOccupiedBytes(schema_->getVersion());
    }
    auto numFields = schema_->getNumFields();
    if (numFields > 0) {
        nullFlags_.resize(((numFields - 1) >> 3) + 1, 0);
    }
    return cord_.size()  // data length
           + offsetBytes * blockOffsets_.size()  // block offsets length
           + nullFlags_.size()  // Number of bytes used for null flags
           + verBytes  // version number length
           + 1;  // Header
}


std::string RowWriter::encode() {
    std::string encoded;

    auto numFields = schema_->getNumFields();
    if (numFields > 0) {
        nullFlags_.resize(((numFields - 1) >> 3) + 1, 0);
    }

    // Reserve enough space so resize will not happen
    encoded.reserve(sizeof(int64_t) * blockOffsets_.size()
                    + cord_.size()
                    + nullFlags_.size()
                    + 11);
    encodeTo(encoded);

    return encoded;
}


void RowWriter::encodeTo(std::string& encoded) {
    if (!schemaWriter_) {
        // Padding the remaining fields. Could throw
        operator<<(Skip(schema_->getNumFields() - colNum_));
    }

    // Header information
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    char header = offsetBytes - 1;

    SchemaVer ver = schema_->getVersion();
    if (ver > 0) {
        auto verBytes = calcOccupiedBytes(ver);
        header |= verBytes << 5;
        encoded.append(&header, 1);
        // Schema version is stored in Little Endian
        encoded.append(reinterpret_cast<char*>(&ver), verBytes);
    } else {
        encoded.append(&header, 1);
    }

    // Null flags follow the header byte
    for (auto& f : nullFlags_) {
        encoded.append(reinterpret_cast<char*>(&f), 1);
    }

    // Offsets follow the Null flags. They are persisted in Little Endian
    for (auto offset : blockOffsets_) {
        encoded.append(reinterpret_cast<char*>(&offset), offsetBytes);
    }

    cord_.appendTo(encoded);
}


Schema RowWriter::moveSchema() {
    if (schemaWriter_) {
        return schemaWriter_->moveSchema();
    } else {
        return Schema();
    }
}


int64_t RowWriter::calcOccupiedBytes(uint64_t v) const noexcept {
    int64_t bytes = 0;
    do {
        bytes++;
        v >>= 8;
    } while (v);

    return bytes;
}


const cpp2::ValueType* RowWriter::getColumnType(
        int64_t colIndex,
        cpp2::SupportedType valueType) noexcept {
    if (colIndex >= static_cast<int64_t>(schema_->getNumFields())) {
        CHECK(!!schemaWriter_) << "SchemaWriter cannot be NULL";
        if (!colType_) {
            colType_.reset(new ColType(valueType));
        }
        return &(colType_->type_);
    } else {
        return &(schema_->getFieldType(colIndex));
    }
}


void RowWriter::writeNullValue(int64_t colIndex, NullValue::NullType type) noexcept {
    // set the Null flag
    size_t byteIdx = colIndex >> 3;
    nullFlags_.resize(byteIdx + 1, 0);
    nullFlags_.back() |= ((uint8_t)0x80 >> (colIndex & 0x00000007));

    // Output the Null value type
    cord_ << static_cast<int8_t>(type);
}


void RowWriter::writeInt(int64_t v) {
    uint8_t buf[10];
    size_t len = folly::encodeVarint(v, buf);
    DCHECK_GT(len, 0UL);
    cord_ << folly::ByteRange(buf, len);
}


void RowWriter::finishWrite() noexcept {
    colNum_++;
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) {
        /* We need to record offset for every 16 fields */
        blockOffsets_.emplace_back(cord_.size());
    }
    if (colNum_ > static_cast<int64_t>(schema_->getNumFields())) {
        /* Need to append the new column type to the schema */
        if (!colName_) {
            schemaWriter_->appendCol(folly::stringPrintf("Column%ld", colNum_),
                                     std::move(colType_->type_));
        } else {
            schemaWriter_->appendCol(std::move(colName_->name_),
                                     std::move(colType_->type_));
        }
    }
    colName_.reset();
    colType_.reset();
}


/****************************
 *
 * Data Stream
 *
 ***************************/
RowWriter& RowWriter::operator<<(bool v) noexcept {
    auto* type = getColumnType(colNum_, cpp2::SupportedType::BOOL);

    switch (type->get_type()) {
        case SupportedType::BOOL:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"bool\"";
            writeNullValue(colNum_, NullValue::NullType::NT_BadType);
            break;
    }

    finishWrite();
    return *this;
}


RowWriter& RowWriter::operator<<(int64_t v) noexcept {
    auto* type = getColumnType(colNum_, cpp2::SupportedType::INT);

    switch (type->get_type()) {
        case cpp2::SupportedType::INT:
        case cpp2::SupportedType::TIMESTAMP: {
            writeInt(v);
            break;
        }
        case cpp2::SupportedType::VID: {
            cord_ << (uint64_t)v;
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"int\"";
            writeNullValue(colNum_, NullValue::NullType::NT_BadType);
            break;
        }
    }

    finishWrite();
    return *this;
}


RowWriter& RowWriter::operator<<(float v) noexcept {
    auto* type = getColumnType(colNum_, cpp2::SupportedType::FLOAT);

    switch (type->get_type()) {
        case SupportedType::FLOAT:
            cord_ << v;
            break;
        case SupportedType::DOUBLE:
            cord_ << static_cast<double>(v);
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"float\"";
            writeNullValue(colNum_, NullValue::NullType::NT_BadType);
            break;
    }

    finishWrite();
    return *this;
}


RowWriter& RowWriter::operator<<(double v) noexcept {
    auto* type = getColumnType(colNum_, cpp2::SupportedType::DOUBLE);

    switch (type->get_type()) {
        case SupportedType::FLOAT:
            cord_ << static_cast<float>(v);
            break;
        case SupportedType::DOUBLE:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"double\"";
            writeNullValue(colNum_, NullValue::NullType::NT_BadType);
            break;
    }

    finishWrite();
    return *this;
}


RowWriter& RowWriter::operator<<(const std::string& v) noexcept {
    return operator<<(folly::StringPiece(v));
}


RowWriter& RowWriter::operator<<(folly::StringPiece v) noexcept {
    auto* type = getColumnType(colNum_, cpp2::SupportedType::STRING);

    switch (type->get_type()) {
        case SupportedType::STRING: {
            writeInt(v.size());
            cord_ << v;
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"string\"";
            writeNullValue(colNum_, NullValue::NullType::NT_BadType);
            break;
        }
    }

    finishWrite();
    return *this;
}


RowWriter& RowWriter::operator<<(const char* v) noexcept {
    return operator<<(folly::StringPiece(v));
}


RowWriter& RowWriter::operator<<(NullValue::NullType nullType) noexcept {
    writeNullValue(colNum_, nullType);
    finishWrite();
    return *this;
}


/****************************
 *
 * Control Stream
 *
 ***************************/
RowWriter& RowWriter::operator<<(ColName&& colName) noexcept {
    DCHECK(!!schemaWriter_) << "ColName can only be used when a schema is missing";
    colName_.reset(new ColName(std::move(colName)));
    return *this;
}


RowWriter& RowWriter::operator<<(ColType&& colType) noexcept {
    DCHECK(!!schemaWriter_) << "ColType can only be used when a schema is missing";
    colType_.reset(new ColType(std::move(colType)));
    return *this;
}


RowWriter& RowWriter::operator<<(Skip&& skip) {
    DCHECK(!schemaWriter_) << "Skip can only be used when a schema is provided";
    if (skip.numColsToSkip_ <= 0) {
        VLOG(2) << "Nothing to skip";
        return *this;
    }

    int32_t skipTo = std::min(colNum_ + skip.numColsToSkip_,
                              static_cast<int64_t>(schema_->getNumFields()));
    for (int i = colNum_; i < skipTo; i++) {
        auto field = schema_->field(i);
        if (!field->nullable()) {
            throw ValueRequiredException();
        }
        if (!field->hasDefault()) {
            // Use NULL value
            writeNullValue(i, NullValue::NullType::NT_Null);
        } else {
            // Use default value
            auto& defVal = field->getDefault();
            switch (field->getType().type) {
                case SupportedType::BOOL: {
                    cord_ << boost::get<bool>(defVal);
                    break;
                }
                case SupportedType::INT:
                case SupportedType::TIMESTAMP: {
                    writeInt(boost::get<int64_t>(defVal));
                    break;
                }
                case SupportedType::FLOAT: {
                    cord_ << boost::get<float>(defVal);
                    break;
                }
                case SupportedType::DOUBLE: {
                    cord_ << boost::get<double>(defVal);
                    break;
                }
                case SupportedType::STRING: {
                    cord_ << boost::get<std::string>(defVal);
                    break;
                }
                case SupportedType::VID: {
                    cord_ << static_cast<uint64_t>(0);
                    break;
                }
                default: {
                    LOG(FATAL) << "Support for this value type has not been implemented";
                }
            }
        }

        // Update block offsets
        if (i != 0 && (i >> 4 << 4) == i) {
            // We need to record block offset for every 16 fields
            blockOffsets_.emplace_back(cord_.size());
        }
    }
    colNum_ = skipTo;

    return *this;
}

}  // namespace nebula
