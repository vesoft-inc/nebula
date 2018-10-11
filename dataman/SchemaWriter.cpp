/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "dataman/SchemaWriter.h"

namespace vesoft {
namespace vgraph {

using namespace storage;

cpp2::Schema SchemaWriter::moveSchema() noexcept {
    cpp2::Schema schema;
    schema.set_columns(std::move(columns_));

    nameIndex_.clear();
    return std::move(schema);
}


SchemaWriter& SchemaWriter::appendCol(const char* name,
                                      cpp2::SupportedType type) noexcept {
    return appendCol(std::string(name), type);
}


SchemaWriter& SchemaWriter::appendCol(const folly::StringPiece name,
                                      cpp2::SupportedType type) noexcept {
    return appendCol(name.toString(), type);
}


SchemaWriter& SchemaWriter::appendCol(std::string&& name,
                                      cpp2::SupportedType type) noexcept {
    cpp2::ValueType vt;
    vt.set_type(type);

    return appendCol(std::move(name), std::move(vt));
}


SchemaWriter& SchemaWriter::appendCol(const char* name,
                                      cpp2::ValueType&& type) noexcept {
    return appendCol(std::string(name), std::move(type));
}


SchemaWriter& SchemaWriter::appendCol(const folly::StringPiece name,
                                      cpp2::ValueType&& type) noexcept {
    return appendCol(name.toString(), std::move(type));
}


SchemaWriter& SchemaWriter::appendCol(std::string&& name,
                                      cpp2::ValueType&& type) noexcept {
    using namespace folly::hash;
    uint64_t hash = SpookyHashV2::Hash64(name.data(), name.size(), 0);
    DCHECK(nameIndex_.find(hash) == nameIndex_.end());

    cpp2::ColumnDef col;
    col.set_name(std::move(name));
    col.set_type(std::move(type));

    columns_.emplace_back(std::move(col));
    nameIndex_.emplace(std::make_pair(hash, columns_.size() - 1));

    return *this;
}

}  // namespace vgraph
}  // namespace vesoft

