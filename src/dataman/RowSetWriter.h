/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_ROWSETWRITER_H_
#define DATAMAN_ROWSETWRITER_H_

#include "base/Base.h"
#include "gen-cpp2/graph_types.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/RowWriter.h"

namespace nebula {

class RowSetWriter {
public:
    // The reservedSize is the potential data size, It hints the writer
    // to reserve the space so that the expensive resize() will not happen
    explicit RowSetWriter(
        std::shared_ptr<const meta::SchemaProviderIf> schema
            = std::shared_ptr<const meta::SchemaProviderIf>(),
        int64_t reservedSize = 4096);

    void setSchema(std::shared_ptr<const meta::SchemaProviderIf> schema) {
        schema_ = std::move(schema);
    }

    std::shared_ptr<const meta::SchemaProviderIf> schema() const {
        return schema_;
    }

    // Return the reference of the rowset data, so that the caller can
    // move the data
    std::string& data() {
        return data_;
    }

    // Both schemas have to be same
    void addRow(RowWriter& writer);
    // Append the encoded row data
    void addRow(const std::string& data);
    // Copy existed rows
    void addAll(const std::string& data);

private:
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::string data_;

    void writeRowLength(int64_t len);
};

}  // namespace nebula
#endif  // DATAMAN_ROWSETWRITER_H_

