/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/RowReaderWrapper.h"

namespace nebula {

bool RowReaderWrapper::reset(meta::SchemaProviderIf const* schema,
                             folly::StringPiece row,
                             int32_t readerVer) noexcept {
    CHECK_NOTNULL(schema);
    if (readerVer == 1) {
        readerV1_.resetImpl(schema, row);
        currReader_ = &readerV1_;
        return true;
    } else if (readerVer == 2) {
        readerV2_.resetImpl(schema, row);
        currReader_ = &readerV2_;
        return true;
    } else {
        LOG(ERROR) << "Unsupported row reader version " << readerVer;
        currReader_ = nullptr;
        return false;
    }
}


// static
void RowReaderWrapper::getVersions(const folly::StringPiece& row,
                                   SchemaVer& schemaVer,
                                   int32_t& readerVer) {
    size_t index = 0;
    if (row.empty()) {
        LOG(WARNING) << "Row data is empty, so there is no version info";
        schemaVer = 0;
        readerVer = 2;
        return;
    }

    readerVer = ((row[index] & 0x18) >> 3) + 1;

    size_t verBytes = 0;
    if (readerVer == 1) {
        // The first three bits indicate the number of bytes for the
        // schema version. If the number is zero, no schema version
        // presents
        verBytes = row[index++] >> 5;
    } else if (readerVer == 2) {
        // The last three bits indicate the number of bytes for the
        // schema version. If the number is zero, no schema version
        // presents
        verBytes = row[index++] & 0x07;
    } else {
        LOG(ERROR) << "Invalid reader version: " << readerVer;
        schemaVer = 0;
    }

    schemaVer = 0;
    if (verBytes > 0) {
        if (verBytes + 1 > row.size()) {
            // Data is too short
            LOG(ERROR) << "Row data is too short: " << toHexStr(row);
            schemaVer = -1;
            return;
        }
        // Schema Version is stored in Little Endian
        memcpy(reinterpret_cast<void*>(&schemaVer), &row[index], verBytes);
    }

    return;
}

}  // namespace nebula

