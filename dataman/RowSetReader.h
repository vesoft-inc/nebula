/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_ROWSETREADER_H_
#define DATAMAN_ROWSETREADER_H_

#include "base/Base.h"
#include "interface/gen-cpp2/vgraph_types.h"
#include "dataman/SchemaProviderIf.h"

namespace vesoft {
namespace vgraph {

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
        const SchemaProviderIf* schema_;
        // The total length of the encoded row set
        const folly::StringPiece& data_;

        std::unique_ptr<RowReader> reader_;
        // The offset of the current row
        int64_t offset_;
        // The length of the current row
        int64_t len_;

        Iterator(SchemaProviderIf* schema,
                 const folly::StringPiece& data,
                 int64_t offset);

        // Prepare the RowReader object
        // When succeeded, the method returns the total length of the row
        // data (including the row length and row data)
        int32_t prepareReader();
    };

public:
    // Constructor to process the execution response
    //
    // In this case, the RowSetReader will take the ownership of the schema
    // and the data
    explicit RowSetReader(cpp2::ExecutionResponse& resp);

    // Constructor to process the property value
    //
    // In this case, the RowSetReader will *NOT* take the ownership of
    // the schema and the record
    RowSetReader(SchemaProviderIf* schema,
                 folly::StringPiece record);

    virtual ~RowSetReader();

    SchemaProviderIf const* schema() const {
        return schema_.get();
    }

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

private:
    std::unique_ptr<SchemaProviderIf> schema_;
    bool takeOwnership_ = false;

    std::string dataStore_;
    folly::StringPiece data_;
};

}  // namepsace vgraph
}  // namespace vesoft
#endif  // DATAMAN_ROWSETREADER_H_

