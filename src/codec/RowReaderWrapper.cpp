/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/RowReaderWrapper.h"

namespace nebula {

// static
RowReaderWrapper RowReaderWrapper::getTagPropReader(meta::SchemaManager* schemaMan,
                                                    GraphSpaceID space,
                                                    TagID tag,
                                                    folly::StringPiece row) {
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer >= 0) {
        auto schema = schemaMan->getTagSchema(space, tag, schemaVer);
        if (schema == nullptr) {
            return RowReaderWrapper();
        }
        return RowReaderWrapper(schema.get(), row, readerVer);
    } else {
        LOG(WARNING) << "Invalid schema version in the row data!";
        return RowReaderWrapper();
    }
}


// static
RowReaderWrapper RowReaderWrapper::getEdgePropReader(meta::SchemaManager* schemaMan,
                                                     GraphSpaceID space,
                                                     EdgeType edge,
                                                     folly::StringPiece row) {
    if (schemaMan == nullptr) {
        LOG(ERROR) << "schemaMan should not be nullptr!";
        return RowReaderWrapper();
    }
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer >= 0) {
        auto schema = schemaMan->getEdgeSchema(space, edge, schemaVer);
        if (schema == nullptr) {
            return RowReaderWrapper();
        }
        return RowReaderWrapper(schema.get(), row, readerVer);
    } else {
        LOG(WARNING) << "Invalid schema version in the row data!";
        return RowReaderWrapper();
    }
}

// static
RowReaderWrapper RowReaderWrapper::getRowReader(const meta::SchemaProviderIf* schema,
                                                folly::StringPiece row) {
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer != schema->getVersion()) {
        return RowReaderWrapper();
    }
    return RowReaderWrapper(schema, row, readerVer);
}

// static
RowReaderWrapper RowReaderWrapper::getRowReader(
        const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
        folly::StringPiece row) {
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (static_cast<size_t>(schemaVer) >= schemas.size() ||
        schemaVer != schemas[schemaVer]->getVersion()) {
        return RowReaderWrapper();
    }
    return RowReaderWrapper(schemas[schemaVer].get(), row, readerVer);
}

RowReaderWrapper::RowReaderWrapper(const meta::SchemaProviderIf* schema,
                                   const folly::StringPiece& row,
                                   int32_t& readerVer)
        : readerVer_(readerVer) {
    CHECK_NOTNULL(schema);
    if (readerVer_ == 1) {
        readerV1_.resetImpl(schema, row);
        currReader_ = &readerV1_;
    } else if (readerVer_ == 2) {
        readerV2_.resetImpl(schema, row);
        currReader_ = &readerV2_;
    } else {
        LOG(FATAL) << "Should not reach here";
    }
}

bool RowReaderWrapper::reset(meta::SchemaProviderIf const* schema,
                             folly::StringPiece row,
                             int32_t readerVer) noexcept {
    CHECK_NOTNULL(schema);
    readerVer_ = readerVer;
    if (readerVer_ == 1) {
        readerV1_.resetImpl(schema, row);
        currReader_ = &readerV1_;
        return true;
    } else if (readerVer_ == 2) {
        readerV2_.resetImpl(schema, row);
        currReader_ = &readerV2_;
        return true;
    } else {
        LOG(ERROR) << "Unsupported row reader version " << readerVer;
        currReader_ = nullptr;
        return false;
    }
}

bool RowReaderWrapper::reset(meta::SchemaProviderIf const* schema,
                             folly::StringPiece row) noexcept {
    currReader_ = nullptr;
    if (schema == nullptr) {
        return false;
    }
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer != schema->getVersion()) {
        return false;
    }
    return reset(schema, row, readerVer);
}

bool RowReaderWrapper::reset(
        const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
        folly::StringPiece row) noexcept {
    currReader_ = nullptr;
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (static_cast<size_t>(schemaVer) >= schemas.size()) {
        return false;
    }
    // the schema is stored from oldest to newest, so just use version as idx
    if (schemaVer != schemas[schemaVer]->getVersion()) {
        return false;
    }
    return reset(schemas[schemaVer].get(), row, readerVer);
}

// static
void RowReaderWrapper::getVersions(const folly::StringPiece& row,
                                   SchemaVer& schemaVer,
                                   int32_t& readerVer) {
    size_t index = 0;
    if (row.empty()) {
        LOG(WARNING) << "Row data is empty, so there is no version info";
        schemaVer = -1;
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
        schemaVer = -1;
        return;
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

