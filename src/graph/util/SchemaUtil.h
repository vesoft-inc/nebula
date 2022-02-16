/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_UTIL_SCHEMAUTIL_H_
#define GRAPH_UTIL_SCHEMAUTIL_H_

#include <memory>  // for shared_ptr, unique_ptr
#include <string>  // for string
#include <vector>  // for vector

#include "common/base/Status.h"    // for Status
#include "common/base/StatusOr.h"  // for StatusOr
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Value.h"  // for Value, Value::Type
#include "common/expression/Expression.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "common/thrift/ThriftTypes.h"        // for EdgeType, GraphSpaceID
#include "interface/gen-cpp2/common_types.h"  // for PropertyType
#include "interface/gen-cpp2/meta_types.h"
#include "interface/gen-cpp2/storage_types.h"  // for EdgeProp, VertexProp
#include "parser/MaintainSentences.h"

namespace nebula {
class Expression;
class SchemaPropItem;
namespace meta {
class NebulaSchemaProvider;
namespace cpp2 {
class ColumnDef;
class ColumnTypeDef;
class Schema;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

class Expression;
class SchemaPropItem;
namespace meta {
class NebulaSchemaProvider;
namespace cpp2 {
class ColumnDef;
class ColumnTypeDef;
class Schema;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

namespace graph {
class QueryContext;
struct SpaceInfo;

class SchemaUtil final {
 public:
  using VertexProp = storage::cpp2::VertexProp;
  using EdgeProp = storage::cpp2::EdgeProp;
  SchemaUtil() = delete;

 public:
  static Status validateProps(const std::vector<SchemaPropItem*>& schemaProps,
                              meta::cpp2::Schema& schema);

  static std::shared_ptr<const meta::NebulaSchemaProvider> generateSchemaProvider(
      const SchemaVer ver, const meta::cpp2::Schema& schema);

  static Status setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

  static Status setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

  static Status setComment(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

  static StatusOr<Value> toVertexID(Expression* expr, Value::Type vidType);

  static StatusOr<std::vector<Value>> toValueVec(std::vector<Expression*> exprs);

  static StatusOr<DataSet> toDescSchema(const meta::cpp2::Schema& schema);

  static StatusOr<DataSet> toShowCreateSchema(bool isTag,
                                              const std::string& name,
                                              const meta::cpp2::Schema& schema);

  static std::string typeToString(const meta::cpp2::ColumnTypeDef& col);
  static std::string typeToString(const meta::cpp2::ColumnDef& col);

  static Value::Type propTypeToValueType(::nebula::cpp2::PropertyType propType);

  static bool isValidVid(const Value& value, const meta::cpp2::ColumnTypeDef& type);

  static bool isValidVid(const Value& value, ::nebula::cpp2::PropertyType type);

  static bool isValidVid(const Value& value);

  // Fetch all tags in the space and retrieve props from tags
  // only take _tag when withProp is false
  static StatusOr<std::unique_ptr<std::vector<::nebula::storage::cpp2::VertexProp>>>
  getAllVertexProp(QueryContext* qctx, GraphSpaceID spaceId, bool withProp);

  // retrieve prop from specific edgetypes
  // only take _src _dst _type _rank when withProp is false
  static StatusOr<std::unique_ptr<std::vector<EdgeProp>>> getEdgeProps(
      QueryContext* qctx,
      const SpaceInfo& space,
      const std::vector<EdgeType>& edgeTypes,
      bool withProp);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_SCHEMAUTIL_H_
