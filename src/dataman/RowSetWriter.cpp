/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "dataman/RowSetWriter.h"

namespace nebula {

using namespace nebula::meta;

RowSetWriter::RowSetWriter(std::shared_ptr<const SchemaProviderIf> schema,
                           int64_t reservedSize)
        : schema_(schema) {
    data_.reserve(reservedSize);
}


void RowSetWriter::writeRowLength(int64_t len) {
    VLOG(3) << "Write row length " << len;
    uint8_t buf[10];
    size_t lenBytes = folly::encodeVarint(len, buf);
    DCHECK_GT(lenBytes, 0UL);
    data_.append(reinterpret_cast<char*>(buf), lenBytes);
}


void RowSetWriter::addRow(RowWriter& writer) {
    writeRowLength(writer.size());
    writer.encodeTo(data_);
}


void RowSetWriter::addRow(const std::string& data) {
    writeRowLength(data.size());
    data_.append(data);
}

}  // namespace nebula

