/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_ROWSETWRITER_H_
#define DATAMAN_ROWSETWRITER_H_

#include "base/Base.h"
#include "gen-cpp2/graph_types.h"
#include "dataman/SchemaProviderIf.h"
#include "dataman/RowWriter.h"

namespace nebula {

class RowSetWriter {
public:
    // The reservedSize is the potential data size, It hints the writer
    // to reserve the space so that the expensive resize() will not happen
    explicit RowSetWriter(const SchemaProviderIf* schema = nullptr,
                          int64_t reservedSize = 4096);

    void setSchema(const SchemaProviderIf* schema) {
        schema_ = schema;
    }

    const SchemaProviderIf* schema() const {
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

private:
    const SchemaProviderIf* schema_;
    std::string data_;

    void writeRowLength(int64_t len);
};

}  // namespace nebula
#endif  // DATAMAN_ROWSETWRITER_H_

