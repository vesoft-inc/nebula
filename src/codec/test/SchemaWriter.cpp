/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "codec/test/SchemaWriter.h"

namespace nebula {

using meta::cpp2::Schema;
using meta::cpp2::PropertyType;
using meta::cpp2::ColumnDef;

Schema SchemaWriter::moveSchema() noexcept {
    Schema schema;
    schema.set_columns(std::move(columns_));

    nameIndex_.clear();
    return schema;
}


SchemaWriter& SchemaWriter::appendCol(folly::StringPiece name,
                                      PropertyType type) noexcept {
    using folly::hash::SpookyHashV2;
    uint64_t hash = SpookyHashV2::Hash64(name.data(), name.size(), 0);
    DCHECK(nameIndex_.find(hash) == nameIndex_.end());

    ColumnDef col;
    col.set_name(name.toString());
    col.set_type(type);

    columns_.emplace_back(std::move(col));
    nameIndex_.emplace(std::make_pair(hash, columns_.size() - 1));

    return *this;
}

}  // namespace nebula

