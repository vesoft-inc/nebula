/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_TEST_ROWWRITERV1_H_
#define CODEC_TEST_ROWWRITERV1_H_

#include "common/base/Base.h"
#include "common/base/ICord.h"
#include "codec/test/SchemaWriter.h"

namespace nebula {

/**
 * It's a write-only data streamer, used to encode one row of data
 *
 * It can only be used with a schema provided
 */
class RowWriterV1 {
public:
    /*******************
     *
     * Stream Control
     *
     ******************/
    // Skip next few columns. Default values will be written for those
    // fields
    // This cannot be used if a schema is not provided
    struct Skip {
        friend class RowWriterV1;
        explicit Skip(int64_t toSkip) : toSkip_(toSkip) {}
    private:
        int64_t toSkip_;
    };

public:
    explicit RowWriterV1(const meta::SchemaProviderIf* schema);

    // Encode into a binary array
    std::string encode() noexcept;
    // Encode and attach to the given string
    // For the sake of performance, th caller needs to make sure the sting
    // is large enough so that resize will not happen
    void encodeTo(std::string& encoded) noexcept;

    // Calculate the exact length of the encoded binary array
    int64_t size() const noexcept;

    const meta::SchemaProviderIf* schema() const {
        return schema_;
    }

    // Data stream
    RowWriterV1& operator<<(bool v) noexcept;
    RowWriterV1& operator<<(float v) noexcept;
    RowWriterV1& operator<<(double v) noexcept;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value, RowWriterV1&>::type
    operator<<(T v) noexcept;

    RowWriterV1& operator<<(const std::string& v) noexcept;
    RowWriterV1& operator<<(folly::StringPiece v) noexcept;
    RowWriterV1& operator<<(const char* v) noexcept;

    // Control stream
    RowWriterV1& operator<<(Skip&& skip) noexcept;

private:
    const meta::SchemaProviderIf* schema_;
    ICord<> cord_;

    int64_t colNum_ = 0;

    // Block offsets for every 16 fields
    std::vector<int64_t> blockOffsets_;

    template<typename T>
    typename std::enable_if<std::is_integral<T>::value>::type
    writeInt(T v);

    // Calculate the number of bytes occupied (ignore the leading 0s)
    int64_t calcOccupiedBytes(uint64_t v) const noexcept;
};

}  // namespace nebula


#include "codec/test/RowWriterV1.inl"

#endif  // CODEC_TEST_ROWWRITERV1_H_



