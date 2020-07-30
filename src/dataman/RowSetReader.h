/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_ROWSETREADER_H_
#define DATAMAN_ROWSETREADER_H_

#include "RowReader.h"
#include "base/Base.h"
#include "gen-cpp2/storage_types.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {

class RowReader;

class RowSetReader {
public:
    class Iterator final {
        friend class RowSetReader;
    public:
        const RowReader& operator*() const noexcept;
        const RowReader* operator->() const noexcept;

        Iterator& operator++() noexcept;

        operator bool() const noexcept;
        bool operator==(const Iterator& rhs);

    private:
        std::shared_ptr<const meta::SchemaProviderIf> schema_;
        // The total length of the encoded row set
        const folly::StringPiece& data_;

        RowReader reader_ = RowReader::getEmptyRowReader();
        // The offset of the current row
        int64_t offset_;
        // The length of the current row
        int64_t len_;

        Iterator(std::shared_ptr<const meta::SchemaProviderIf> schema,
                 const folly::StringPiece& data,
                 int64_t offset);

        // Prepare the RowReader object
        // When succeeded, the method returns the total length of the row
        // data (including the row length and row data)
        int32_t prepareReader();
    };

public:
    // Constructor to process the property value
    //
    // In this case, the RowSetReader will *NOT* take the ownership of
    // the schema and the record
    RowSetReader(std::shared_ptr<const meta::SchemaProviderIf> schema,
                 folly::StringPiece record);

    virtual ~RowSetReader() = default;

    std::shared_ptr<const meta::SchemaProviderIf> schema() const {
        return schema_;
    }

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

    folly::StringPiece getData() const noexcept {
        return data_;
    }

private:
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::string dataStore_;
    folly::StringPiece data_;
};

}  // namespace nebula
#endif  // DATAMAN_ROWSETREADER_H_

