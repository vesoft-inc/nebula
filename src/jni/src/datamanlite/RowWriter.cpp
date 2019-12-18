/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datamanlite/RowWriter.h"


namespace nebula {
namespace dataman {
namespace codec {

int64_t RowWriter::size() const noexcept {
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    SchemaVer verBytes = 0;
    if (ver_ > 0) {
        verBytes = calcOccupiedBytes(ver_);
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

    return encoded;
}


void RowWriter::encodeTo(std::string& encoded) noexcept {
    // Header information
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    char header = offsetBytes - 1;

    SchemaVer ver = ver_;
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


int64_t RowWriter::calcOccupiedBytes(uint64_t v) const noexcept {
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
RowWriter& RowWriter::operator<<(bool v) noexcept {
    cord_ << v;
    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(float v) noexcept {
    cord_ << v;
    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(double v) noexcept {
    cord_ << v;
    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(const std::string& v) noexcept {
    return operator<<(Slice(v));
}

RowWriter& RowWriter::operator<<(Slice v) noexcept {
    writeInt(v.size());
    cord_.write(v.data(), v.size());
    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(const char* v) noexcept {
    return operator<<(Slice(v));
}


}  // namespace codec
}  // namespace dataman
}  // namespace nebula
