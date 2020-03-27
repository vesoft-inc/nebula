/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "codec/RowReader.h"
#include "codec/RowReaderV1.h"

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
        folly::StringPiece row) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::unique_ptr<RowReader>(
            new RowReaderV1(row, schemaMan->getTagSchema(space, tag, ver)));
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
        folly::StringPiece row) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::unique_ptr<RowReader>(
            new RowReaderV1(row, schemaMan->getEdgeSchema(space, edge, ver)));
    }

    // Invalid data
    // TODO We need a better error handler here
    LOG(FATAL) << "Invalid schema version in the row data!";
}
*/

// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        std::shared_ptr<const meta::SchemaProviderIf> schema,
        folly::StringPiece row) {
    SchemaVer ver = getSchemaVer(row);
    CHECK_EQ(ver, schema->getVersion());
    return std::unique_ptr<RowReader>(new RowReaderV1(row, std::move(schema)));
}


// static
SchemaVer RowReader::getSchemaVer(folly::StringPiece row) {
    const uint8_t* it = reinterpret_cast<const uint8_t*>(row.begin());
    if (reinterpret_cast<const char*>(it) == row.end()) {
        LOG(ERROR) << "Row data is empty, so there is no schema version";
        return 0;
    }

    // The first three bits indicate the number of bytes for the
    // schema version. If the number is zero, no schema version
    // presents
    size_t verBytes = *(it++) >> 5;
    int32_t ver = 0;
    if (verBytes > 0) {
        if (verBytes + 1 > row.size()) {
            // Data is too short
            LOG(ERROR) << "Row data is too short: " << toHexStr(row);
            return 0;
        }
        // Schema Version is stored in Little Endian
        for (size_t i = 0; i < verBytes; i++) {
            ver |= (uint32_t(*(it++)) << (8 * i));
        }
    }

    return ver;
}


const Value& RowReader::getDefaultValue(const std::string& prop) {
    auto field = schema_->field(prop);
    if (!!field && field->hasDefault()) {
        return field->defaultValue();
    }

    return Value::null();
}

}  // namespace nebula
