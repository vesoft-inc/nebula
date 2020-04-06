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
/*
std::unique_ptr<RowReader> RowReader::getTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        std::string row) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::unique_ptr<RowReader>(
            new RowReaderV1(std::move(row), schemaMan->getTagSchema(space, tag, ver)));
    }

    // Invalid data
    // TODO We need a better error handler here
    LOG(FATAL) << "Invalid schema version in the row data!";
}


// static
std::unique_ptr<RowReader> RowReader::getEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        std::string row) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::unique_ptr<RowReader>(
            new RowReaderV1(st::move(row), schemaMan->getEdgeSchema(space, edge, ver)));
    }

    // Invalid data
    // TODO We need a better error handler here
    LOG(FATAL) << "Invalid schema version in the row data!";
}
*/

// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        const meta::SchemaProviderIf* schema,
        folly::StringPiece row) {
    auto reader = std::make_unique<RowReaderWrapper>();
    if (reader->reset(schema, row)) {
        return reader;
    } else {
        LOG(ERROR) << "Failed to initiate the reader, most likely the data"
                      "is corrupted. The data is ["
                   << toHexStr(row)
                   << "]";
        return std::unique_ptr<RowReader>();
    }
}


bool RowReader::resetImpl(meta::SchemaProviderIf const* schema,
                          folly::StringPiece row) noexcept {
    schema_ = schema;
    data_ = row;

    endIter_.reset(schema_->getNumFields());
    return true;
}

}  // namespace nebula
