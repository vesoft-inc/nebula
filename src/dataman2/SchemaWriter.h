/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN2_SCHEMAWRITER_H_
#define DATAMAN2_SCHEMAWRITER_H_

#include "base/Base.h"
#include "dataman2/ResultSchemaProvider.h"

namespace nebula {

class SchemaWriter : public ResultSchemaProvider {
public:
    explicit SchemaWriter(SchemaVer ver = 0) : ResultSchemaProvider(ver) {}

    // Move the schema out of the writer
    cpp2::Schema moveSchema() noexcept;

    SchemaWriter& appendCol(folly::StringPiece name,
                            cpp2::SupportedType type) noexcept;

    SchemaWriter& appendCol(folly::StringPiece name,
                            cpp2::ValueType&& type)noexcept;

private:
};

}  // namespace nebula
#endif  // DATAMAN2_SCHEMAWRITER_H_

