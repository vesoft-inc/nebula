/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SCHEMAHELPER_H_
#define GRAPH_SCHEMAHELPER_H_

#include "base/Base.h"
#include "base/Status.h"
#include "gen-cpp2/common_types.h"
#include "parser/MaintainSentences.h"

/**
 * SchemaHelper is the interface of `create schema' used in `create tag/edge' and `alter schema'
 * used in `alter tag/edge'.
 */

namespace nebula {
namespace graph {

class SchemaHelper final {
public:
    static Status createSchema(const std::vector<ColumnSpecification*>& specs,
                               const std::vector<SchemaPropItem*>& schemaProps,
                               nebula::cpp2::Schema& schema);

    static Status setTTLDuration(SchemaPropItem* schemaProp, nebula::cpp2::Schema& schema);

    static Status setTTLCol(SchemaPropItem* schemaProp, nebula::cpp2::Schema& schema);

    static Status alterSchema(const std::vector<AlterSchemaOptItem*>& schemaOpts,
                              const std::vector<SchemaPropItem*>& schemaProps,
                              std::vector<nebula::meta::cpp2::AlterSchemaItem>& options,
                              nebula::cpp2::SchemaProp& schemaProp);

    static nebula::cpp2::SupportedType columnTypeToSupportedType(nebula::ColumnType type);

private:
    static StatusOr<nebula::cpp2::Value> toDefaultValue(ColumnSpecification* spec);
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_SCHEMAHELPER_H_
