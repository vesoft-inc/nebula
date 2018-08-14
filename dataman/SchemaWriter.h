/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_SCHEMAWRITER_H_
#define DATAMAN_SCHEMAWRITER_H_

#include "base/Base.h"
#include "dataman/ResultSchemaProvider.h"

namespace vesoft {
namespace vgraph {

class SchemaWriter : public ResultSchemaProvider {
public:
    SchemaWriter() = default;

    // Move the schema out of the writer
    cpp2::Schema moveSchema() noexcept;

    SchemaWriter& appendCol(const char* name,
                            cpp2::SupportedType type) noexcept;
    SchemaWriter& appendCol(const folly::StringPiece name,
                            cpp2::SupportedType type) noexcept;
    SchemaWriter& appendCol(std::string&& name,
                            cpp2::SupportedType type) noexcept;

    SchemaWriter& appendCol(const char* name,
                            cpp2::ValueType&& type)noexcept;
    SchemaWriter& appendCol(const folly::StringPiece name,
                            cpp2::ValueType&& type)noexcept;
    SchemaWriter& appendCol(std::string&& name,
                            cpp2::ValueType&& type)noexcept;

private:
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // DATAMAN_SCHEMAWRITER_H_

