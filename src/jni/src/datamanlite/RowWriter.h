/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_CODEC_ROWWRITER_H_
#define DATAMAN_CODEC_ROWWRITER_H_

#include <type_traits>
#include <vector>
#include "base/Cord.h"
#include "datamanlite/DataCommon.h"
#include "datamanlite/Slice.h"
#include "datamanlite/SchemaProviderIf.h"

#define RW_CLEAN_UP_WRITE() \
    colNum_++; \
    if (colNum_ != 0 && (colNum_ >> 4 << 4) == colNum_) { \
        /* We need to record offset for every 16 fields */ \
        blockOffsets_.emplace_back(cord_.size()); \
    }


namespace nebula {
namespace dataman {
namespace codec {

/**
 * It's a write-only data streamer, used to encode one row of data
 * It just encodes the data writed-in.
 */
class RowWriter {
public:
    explicit RowWriter(SchemaVer ver = 0)
        : ver_(ver) {}

    // Encode into a binary array
    std::string encode() noexcept;
    // Encode and attach to the given string
    // For the sake of performance, th caller needs to make sure the sting
    // is large enough so that resize will not happen
    void encodeTo(std::string& encoded) noexcept;

    // Calculate the exact length of the encoded binary array
    int64_t size() const noexcept;

    // Data stream
    RowWriter& operator<<(bool v) noexcept;
    RowWriter& operator<<(float v) noexcept;
    RowWriter& operator<<(double v) noexcept;

    /*
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, RowWriter&>::type
    operator<<(T v) noexcept;
    */
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, RowWriter&>::type
    operator<<(T v) noexcept {
        writeInt(v);
        RW_CLEAN_UP_WRITE()
        return *this;
    }


    RowWriter& operator<<(const std::string& v) noexcept;
    RowWriter& operator<<(Slice v) noexcept;
    RowWriter& operator<<(const char* v) noexcept;

private:
    nebula::Cord cord_;
    SchemaVer ver_;

    int64_t colNum_ = 0;
    // Block offsets for every 16 fields
    std::vector<int64_t> blockOffsets_;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value>::type
    writeInt(T v) {
        uint8_t buf[10];
        size_t len = encodeVarint(v, buf);
        DCHECK_GT(len, 0UL);
        cord_.write(reinterpret_cast<const char*>(&buf[0]), len);
    }


    // Calculate the number of bytes occupied (ignore the leading 0s)
    int64_t calcOccupiedBytes(uint64_t v) const noexcept;
};

}  // namespace codec
}  // namespace dataman
}  // namespace nebula

#endif  // DATAMAN_CODEC_ROWWRITER_H_



