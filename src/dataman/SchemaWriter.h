/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_SCHEMAWRITER_H_
#define DATAMAN_SCHEMAWRITER_H_

#include "dataman/ResultSchemaProvider.h"

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
#endif  // DATAMAN_SCHEMAWRITER_H_

