/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "codec/RowReader.h"
#include "codec/RowReaderWrapper.h"

namespace nebula {

/*********************************************
 *
 * class RowReader::Cell
 *
 ********************************************/
Value RowReader::Cell::value() const noexcept {
    return iter_->reader_->getValueByIndex(iter_->index_);
}


/*********************************************
 *
 * class RowReader::Iterator
 *
 ********************************************/
RowReader::Iterator::Iterator(Iterator&& iter)
    : reader_(iter.reader_)
    , cell_(std::move(iter.cell_))
    , index_(iter.index_) {
}


void RowReader::Iterator::operator=(Iterator&& rhs) {
    reader_ = rhs.reader_;
    cell_ = std::move(rhs.cell_);
    index_ = rhs.index_;
}


bool RowReader::Iterator::operator==(const Iterator& rhs) const noexcept {
    return reader_ == rhs.reader_ && index_ == rhs.index_;
}


const RowReader::Cell& RowReader::Iterator::operator*() const noexcept {
    return cell_;
}


const RowReader::Cell* RowReader::Iterator::operator->() const noexcept {
    return &cell_;
}


RowReader::Iterator& RowReader::Iterator::operator++() {
    if (index_ < reader_->numFields()) {
        ++index_;
    }
    return *this;
}


/*********************************************
 *
 * class RowReader
 *
 ********************************************/

// static
std::unique_ptr<RowReader> RowReader::getTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        folly::StringPiece row) {
    auto reader = std::make_unique<RowReaderWrapper>();
    if (reader->resetTagPropReader(schemaMan, space, tag, row)) {
        return reader;
    }
    LOG(ERROR) << "Failed to initiate the reader, most likely the data"
                      "is corrupted. The data is ["
                   << toHexStr(row)
                   << "]";
    return std::unique_ptr<RowReader>();
}


// static
std::unique_ptr<RowReader> RowReader::getEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        folly::StringPiece row) {
    auto reader = std::make_unique<RowReaderWrapper>();
    if (reader->resetEdgePropReader(schemaMan, space, edge, row)) {
        return reader;
    }
    LOG(ERROR) << "Failed to initiate the reader, most likely the data"
                      "is corrupted. The data is ["
                   << toHexStr(row)
                   << "]";
    return std::unique_ptr<RowReader>();
}

// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        const meta::SchemaProviderIf* schema,
        folly::StringPiece row) {
    auto reader = std::make_unique<RowReaderWrapper>();
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer != schema->getVersion()) {
        return std::unique_ptr<RowReader>();
    }
    if (reader->reset(schema, row, readerVer)) {
        return reader;
    } else {
        LOG(ERROR) << "Failed to initiate the reader, most likely the data"
                      "is corrupted. The data is ["
                   << toHexStr(row)
                   << "]";
        return std::unique_ptr<RowReader>();
    }
}

// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
        folly::StringPiece row) {
    auto reader = std::make_unique<RowReaderWrapper>();
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (static_cast<size_t>(schemaVer) >= schemas.size()) {
        return std::unique_ptr<RowReader>();
    }
    // the schema is stored from oldest to newest, so just use version as idx
    if (schemaVer != schemas[schemaVer]->getVersion()) {
        return std::unique_ptr<RowReader>();
    }
    if (reader->reset(schemas[schemaVer].get(), row, readerVer)) {
        return reader;
    } else {
        LOG(ERROR) << "Failed to initiate the reader, most likely the data"
                      "is corrupted. The data is ["
                   << toHexStr(row)
                   << "]";
        return std::unique_ptr<RowReader>();
    }
}

bool RowReader::resetTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        folly::StringPiece row) {
    if (schemaMan == nullptr) {
        LOG(ERROR) << "schemaMan should not be nullptr!";
        return false;
    }
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer >= 0) {
        auto schema = schemaMan->getTagSchema(space, tag, schemaVer);
        if (schema == nullptr) {
            return false;
        }
        return reset(schema.get(), row, readerVer);
    } else {
        LOG(WARNING) << "Invalid schema version in the row data!";
        return false;
    }
}

bool RowReader::resetEdgePropReader(
            meta::SchemaManager* schemaMan,
            GraphSpaceID space,
            EdgeType edge,
            folly::StringPiece row) {
    if (schemaMan == nullptr) {
        LOG(ERROR) << "schemaMan should not be nullptr!";
        return false;
    }
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer >= 0) {
        auto schema = schemaMan->getEdgeSchema(space, edge, schemaVer);
        if (schema == nullptr) {
            return false;
        }
        return reset(schema.get(), row, readerVer);
    } else {
        LOG(WARNING) << "Invalid schema version in the row data!";
        return false;
    }
}

bool RowReader::reset(meta::SchemaProviderIf const* schema,
                      folly::StringPiece row) noexcept {
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

bool RowReader::reset(const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
                      folly::StringPiece row) noexcept {
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

bool RowReader::resetImpl(meta::SchemaProviderIf const* schema,
                          folly::StringPiece row) noexcept {
    schema_ = schema;
    data_ = row;

    endIter_.reset(schema_->getNumFields());
    return true;
}

}  // namespace nebula
