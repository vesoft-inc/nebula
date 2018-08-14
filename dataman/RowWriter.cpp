/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "dataman/RowWriter.h"

namespace vesoft {
namespace vgraph {

RowWriter::RowWriter(SchemaProviderIf* schema, int32_t schemaVer)
        : schema_(schema)
        , schemaVer_(schemaVer) {
    if (!schema_) {
        // Need to create a new schema
        schemaWriter_.reset(new SchemaWriter());
        schema_ = schemaWriter_.get();
    }
}


int64_t RowWriter::size() const noexcept {
    int32_t offsetBytes = calcOccupiedBytes(cord_.size());
    int32_t verBytes = 0;
    if (schemaVer_) {
        verBytes = calcOccupiedBytes(schemaVer_);
    }
    return cord_.size()  // data length
           + offsetBytes * blockOffsets_.size()  // block offsets length
           + verBytes  // version number length
           + 1;  // Header
}


std::string RowWriter::encode() noexcept {
    std::string encoded;
    // Reserve enough space so resize will not happen
    encoded.reserve(sizeof(int64_t) * blockOffsets_.size() + cord_.size() + 11);
    encodeTo(encoded);

    return std::move(encoded);
}


void RowWriter::encodeTo(std::string& encoded) noexcept {
    if (!schemaWriter_) {
        operator<<(Skip(schema_->getNumFields(schemaVer_) - colNum_));
    }

    // Header information
    int32_t offsetBytes = calcOccupiedBytes(cord_.size());
    char header = offsetBytes - 1;

    if (schemaVer_) {
        size_t verBytes = calcOccupiedBytes(schemaVer_);
        header |= verBytes << 5;
        encoded.append(&header, 1);
        // Schema version is stored in Little Endian
        encoded.append(reinterpret_cast<char*>(&schemaVer_), verBytes);
    } else {
        encoded.append(&header, 1);
    }

    // Offsets are stored in Little Endian
    for (auto offset : blockOffsets_) {
        encoded.append(reinterpret_cast<char*>(&offset), offsetBytes);
    }

    cord_.appendTo(encoded);
}


cpp2::Schema RowWriter::moveSchema() {
    if (schemaWriter_) {
        return std::move(schemaWriter_->moveSchema());
    } else {
        return cpp2::Schema();
    }
}


int32_t RowWriter::calcOccupiedBytes(uint64_t v) const noexcept {
    int32_t bytes = 0;
    do {
        bytes++;
        v >>= 8;
    } while (v);

    return bytes;
}


/****************************
 *
 * Data Stream
 *
 ***************************/
RowWriter& RowWriter::operator<<(bool v) noexcept {
    RW_GET_COLUMN_TYPE(BOOL)

    switch (type->get_type()) {
        case cpp2::SupportedType::BOOL:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"bool\"";
            // Output a default value
            cord_ << false;
            break;
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(float v) noexcept {
    RW_GET_COLUMN_TYPE(FLOAT)

    switch (type->get_type()) {
        case cpp2::SupportedType::FLOAT:
            cord_ << v;
            break;
        case cpp2::SupportedType::DOUBLE:
            cord_ << static_cast<double>(v);
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"float\"";
            cord_ << (float)0.0;
            break;
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(double v) noexcept {
    RW_GET_COLUMN_TYPE(DOUBLE)

    switch (type->get_type()) {
        case cpp2::SupportedType::FLOAT:
            cord_ << static_cast<float>(v);
            break;
        case cpp2::SupportedType::DOUBLE:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"double\"";
            cord_ << (double)0.0;
            break;
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(const std::string& v) noexcept {
    return operator<<(folly::StringPiece(v));
}


RowWriter& RowWriter::operator<<(folly::StringPiece v) noexcept {
    RW_GET_COLUMN_TYPE(STRING)

    switch (type->get_type()) {
        case cpp2::SupportedType::STRING: {
            writeInt(v.size());
            cord_ << v;
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"string\"";
            writeInt(0);
            break;
        }
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(const char* v) noexcept {
    return operator<<(folly::StringPiece(v));
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


RowWriter& RowWriter::operator<<(Skip&& skip) noexcept {
    DCHECK(!schemaWriter_) << "Skip can only be used when a schema is provided";
    if (skip.toSkip_ <= 0) {
        VLOG(2) << "Nothing to skip";
        return *this;
    }

    int32_t skipTo = std::min(colNum_ + skip.toSkip_,
                              schema_->getNumFields(schemaVer_));
    for (int i = colNum_; i < skipTo; i++) {
        switch (schema_->getFieldType(i, schemaVer_)->get_type()) {
            case cpp2::SupportedType::BOOL: {
                cord_ << false;
                break;
            }
            case cpp2::SupportedType::INT: {
                writeInt(0);
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                cord_ << (float)0.0;
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                cord_ << (double)0.0;
                break;
            }
            case cpp2::SupportedType::STRING: {
                writeInt(0);
                break;
            }
            case cpp2::SupportedType::VID: {
                cord_ << (uint64_t)0;
                break;
            }
            default: {
                LOG(FATAL) << "Support for this value type has not been implemented";
            }
        }

        // Update block offsets
        if (i != 0 && (i >> 4 << 4) == i) {
            // We need to record block offset for every 16 fields
            blockOffsets_.push_back(cord_.size());
        }
    }
    colNum_ = skipTo;

    return *this;
}

}  // namespace vgraph
}  // namespace vesoft

