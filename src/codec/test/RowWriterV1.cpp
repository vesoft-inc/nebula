/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/test/RowWriterV1.h"

namespace nebula {

using meta::cpp2::Schema;
using meta::cpp2::PropertyType;
using meta::SchemaProviderIf;

RowWriterV1::RowWriterV1(const SchemaProviderIf* schema)
        : schema_(schema) {
    CHECK(!!schema_);
}


int64_t RowWriterV1::size() const noexcept {
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    SchemaVer verBytes = 0;
    if (schema_->getVersion() > 0) {
        verBytes = calcOccupiedBytes(schema_->getVersion());
    }
    return cord_.size()  // data length
           + offsetBytes * blockOffsets_.size()  // block offsets length
           + verBytes  // version number length
           + 1;  // Header
}


std::string RowWriterV1::encode() noexcept {
    std::string encoded;
    // Reserve enough space so resize will not happen
    encoded.reserve(sizeof(int64_t) * blockOffsets_.size() + cord_.size() + 11);
    encodeTo(encoded);

    return encoded;
}


void RowWriterV1::encodeTo(std::string& encoded) noexcept {
    operator<<(Skip(schema_->getNumFields() - colNum_));

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

    // Offsets are stored in Little Endian
    for (auto offset : blockOffsets_) {
        encoded.append(reinterpret_cast<char*>(&offset), offsetBytes);
    }

    cord_.appendTo(encoded);
}


int64_t RowWriterV1::calcOccupiedBytes(uint64_t v) const noexcept {
    int64_t bytes = 0;
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
RowWriterV1& RowWriterV1::operator<<(bool v) noexcept {
    switch (schema_->getFieldType(colNum_)) {
        case PropertyType::BOOL:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"bool\"";
            // Output a default value
            cord_ << false;
            break;
    }

    colNum_++;
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) {
        /* We need to record offset for every 16 fields */
        blockOffsets_.emplace_back(cord_.size());
    }

    return *this;
}


RowWriterV1& RowWriterV1::operator<<(float v) noexcept {
    switch (schema_->getFieldType(colNum_)) {
        case PropertyType::FLOAT:
            cord_ << v;
            break;
        case PropertyType::DOUBLE:
            cord_ << static_cast<double>(v);
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"float\"";
            cord_ << static_cast<float>(0.0);
            break;
    }

    colNum_++;
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) {
        /* We need to record offset for every 16 fields */
        blockOffsets_.emplace_back(cord_.size());
    }

    return *this;
}


RowWriterV1& RowWriterV1::operator<<(double v) noexcept {
    switch (schema_->getFieldType(colNum_)) {
        case PropertyType::FLOAT:
            cord_ << static_cast<float>(v);
            break;
        case PropertyType::DOUBLE:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"double\"";
            cord_ << static_cast<double>(0.0);
            break;
    }

    colNum_++;
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) {
        /* We need to record offset for every 16 fields */
        blockOffsets_.emplace_back(cord_.size());
    }

    return *this;
}


RowWriterV1& RowWriterV1::operator<<(const std::string& v) noexcept {
    return operator<<(folly::StringPiece(v));
}


RowWriterV1& RowWriterV1::operator<<(const char* v) noexcept {
    return operator<<(folly::StringPiece(v));
}


RowWriterV1& RowWriterV1::operator<<(folly::StringPiece v) noexcept {
    switch (schema_->getFieldType(colNum_)) {
        case PropertyType::STRING: {
            writeInt(v.size());
            cord_.write(v.data(), v.size());
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"string\"";
            writeInt(0);
            break;
        }
    }

    colNum_++;
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) {
        /* We need to record offset for every 16 fields */
        blockOffsets_.emplace_back(cord_.size());
    }

    return *this;
}


/****************************
 *
 * Control Stream
 *
 ***************************/
RowWriterV1& RowWriterV1::operator<<(Skip&& skip) noexcept {
    if (skip.toSkip_ <= 0) {
        VLOG(2) << "Nothing to skip";
        return *this;
    }

    int32_t skipTo = std::min(colNum_ + skip.toSkip_,
                              static_cast<int64_t>(schema_->getNumFields()));
    for (int i = colNum_; i < skipTo; i++) {
        switch (schema_->getFieldType(i)) {
            case PropertyType::BOOL: {
                cord_ << false;
                break;
            }
            case PropertyType::INT64:
            case PropertyType::TIMESTAMP: {
                writeInt(0);
                break;
            }
            case PropertyType::FLOAT: {
                cord_ << static_cast<float>(0.0);
                break;
            }
            case PropertyType::DOUBLE: {
                cord_ << static_cast<double>(0.0);
                break;
            }
            case PropertyType::STRING: {
                writeInt(0);
                break;
            }
            case PropertyType::VID: {
                cord_ << static_cast<uint64_t>(0);
                break;
            }
            default: {
                LOG(FATAL) << "Support for this value type has not been implemented";
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
