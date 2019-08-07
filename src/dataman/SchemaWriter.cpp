/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman/SchemaWriter.h"

namespace nebula {

using cpp2::Schema;
using cpp2::ValueType;
using cpp2::SupportedType;
using cpp2::ColumnDef;

Schema SchemaWriter::moveSchema() noexcept {
    Schema schema;
    schema.set_columns(std::move(columns_));

    nameIndex_.clear();
    return schema;
}


SchemaWriter& SchemaWriter::appendCol(folly::StringPiece name,
                                      SupportedType type) noexcept {
    ValueType vt;
    vt.set_type(type);

    return appendCol(name, std::move(vt));
}


SchemaWriter& SchemaWriter::appendCol(folly::StringPiece name,
                                      ValueType&& type) noexcept {
    using folly::hash::SpookyHashV2;
    uint64_t hash = SpookyHashV2::Hash64(name.data(), name.size(), 0);
    DCHECK(nameIndex_.find(hash) == nameIndex_.end());

    ColumnDef col;
    col.set_name(name.toString());
    col.set_type(std::move(type));

    columns_.emplace_back(std::move(col));
    nameIndex_.emplace(std::make_pair(hash, columns_.size() - 1));

    return *this;
}

}  // namespace nebula

