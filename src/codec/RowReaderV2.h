/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADERV2_H_
#define CODEC_ROWREADERV2_H_

#include "common/base/Base.h"
#include "common/meta/SchemaProviderIf.h"
#include "codec/RowReader.h"
#include <gtest/gtest_prod.h>

namespace nebula {

class RowReaderWrapper;

/**
 * This class decodes the data from version 2.0
 */
class RowReaderV2 : public RowReader {
    friend class RowReaderWrapper;

    FRIEND_TEST(RowReaderV2, encodedData);
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);

public:
    virtual ~RowReaderV2() = default;

    Value getValueByName(const std::string& prop) const noexcept override;
    Value getValueByIndex(const int64_t index) const noexcept override;
    int64_t getTimestamp() const noexcept override;

    int32_t readerVer() const noexcept override {
        return 2;
    }

    size_t headerLen() const noexcept override {
        return headerLen_;
    }

protected:
    bool resetImpl(meta::SchemaProviderIf const* schema, folly::StringPiece row)
        noexcept override;

private:
    size_t headerLen_;
    size_t numNullBytes_;

    RowReaderV2() = default;

    // Check whether the flag at the given position is set or not
    bool isNull(size_t pos) const;
};

}  // namespace nebula
#endif  // CODEC_ROWREADERV2_H_

